from typing import Dict

import numpy as np
import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, data_values, models


class HealthcareVisits:
    """Manages healthcare utilization"""

    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self):
        pass

    def __repr__(self):
        return "HealthcareVisits"

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)
        self.sbp = builder.value.get_value("high_systolic_blood_pressure.exposure")
        self.ldlc = builder.value.get_value("high_ldl_cholesterol.exposure")

        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(
            utilization_data, parameter_columns=["age", "year"], key_columns=["sex"]
        )
        self.background_utilization_rate = builder.value.register_rate_producer(
            "utilization_rate", background_utilization_rate, requires_columns=["age", "sex"]
        )

        # Add columns
        self.visit_type_column = data_values.COLUMNS.VISIT_TYPE
        self.scheduled_visit_date_column = data_values.COLUMNS.SCHEDULED_VISIT_DATE
        # FIXME [MIC-3457]: doc update changes missed appointment probability
        self.miss_scheduled_visit_probability_column = (
            data_values.COLUMNS.MISS_SCHEDULED_VISIT_PROBABILITY
        )
        self.sbp_medication_column = data_values.COLUMNS.SBP_MEDICATION
        self.sbp_medication_adherence_type_column = (
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE
        )
        self.ldlc_medication_column = data_values.COLUMNS.LDLC_MEDICATION
        self.ldlc_medication_adherence_type_column = (
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE
        )

        columns_created = [
            self.visit_type_column,
            self.scheduled_visit_date_column,
            self.miss_scheduled_visit_probability_column,
            self.sbp_medication_column,
            self.sbp_medication_adherence_type_column,
            self.ldlc_medication_column,
            self.ldlc_medication_adherence_type_column,
        ]
        columns_required = [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.MYOCARDIAL_INFARCTION_MODEL_NAME,
        ]
        self.population_view = builder.population.get_view(columns_required + columns_created)

        values_required = [
            "high_systolic_blood_pressure.exposure",
            "high_ldl_cholesterol.exposure",
        ]

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
            requires_columns=columns_required,
            requires_values=values_required,
        )

        # Register listeners
        builder.event.register_listener("time_step__cleanup", self.on_time_step_cleanup)

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """All simulants on SBP medication, LDL-C medication, or a history of
        an acute event (ie intialized in state post-MI or chronic IS) should be
        initialized with a scheduled followup visit 0-6 months out, uniformly
        distributed. All simulants initialized in an acute state should be
        scheduled a followup visit 3-6 months out, uniformly distributed.

        For simplicity, do not assign background screenings on initialization.

        A burn-in period allows for the observed simulation to start with more
        realistic treatments and scheduled followups.
        """
        index = pop_data.index
        event_time = self.clock() + self.step_size()
        df = self.population_view.subview(
            [
                "age",
                "sex",
                models.ISCHEMIC_STROKE_MODEL_NAME,
                models.MYOCARDIAL_INFARCTION_MODEL_NAME,
            ]
        ).get(pop_data.index)

        # Initialize new columns
        df[self.visit_type_column] = data_values.VISIT_TYPE.NONE
        df[self.scheduled_visit_date_column] = pd.NaT
        # FIXME [MIC-3457]: doc update changes missed appointment probability
        lower = data_values.MISS_SCHEDULED_VISIT_PROBABILITY_MIN
        upper = data_values.MISS_SCHEDULED_VISIT_PROBABILITY_MAX
        df[self.miss_scheduled_visit_probability_column] = lower + (
            upper - lower
        ) * self.randomness.get_draw(index)
        df[self.sbp_medication_column] = np.nan
        df[self.sbp_medication_adherence_type_column] = np.nan
        df[self.ldlc_medication_column] = np.nan
        df[self.ldlc_medication_adherence_type_column] = np.nan

        df = self.initialize_medication_coverage(df)
        # breakpoint()

        # Handle simulants initialized in an post/chronic state
        visitors = {}
        mask_chronic_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = index[mask_chronic_is | mask_post_mi]
        visitors[data_values.VISIT_TYPE.SCHEDULED] = acute_history
        df.loc[acute_history] = self.visit_doctor(
            df=df.loc[acute_history],
            visitors=visitors,
            event_time=event_time,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )

        # Handle simulants initialized in an emergency state
        visitors = {}
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        emergency_not_already_scheduled = index[
            (mask_acute_is | mask_acute_mi) & ~(mask_chronic_is | mask_post_mi)
        ]
        visitors[data_values.VISIT_TYPE.EMERGENCY] = emergency_not_already_scheduled
        df.loc[emergency_not_already_scheduled] = self.visit_doctor(
            df=df.loc[emergency_not_already_scheduled],
            visitors=visitors,
            event_time=event_time,
        )

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
                    self.miss_scheduled_visit_probability_column,
                    self.sbp_medication_column,
                    self.sbp_medication_adherence_type_column,
                    self.ldlc_medication_column,
                    self.ldlc_medication_adherence_type_column,
                ]
            ]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit. In the event that a simulant goes for an emergency or
        background visit but already has a followup visit scheduled for the future,
        keep that scheduled followup and do not schedule another one.
        """
        df = self.population_view.get(event.index, query='alive == "alive"')
        visitors = {}

        # Emergency visits
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        visit_emergency = df[mask_emergency].index
        visitors[data_values.VISIT_TYPE.EMERGENCY] = visit_emergency

        # Scheduled visits
        mask_scheduled_non_emergency = (
            (df[self.scheduled_visit_date_column] > (event.time - event.step_size))
            & (df[self.scheduled_visit_date_column] <= event.time)
            & (~mask_emergency)
        )
        scheduled_non_emergency = df[mask_scheduled_non_emergency].index
        # Missed scheduled (non-emergency) visits (these do not get re-scheduled)
        missed_visit = self.randomness.filter_for_probability(
            scheduled_non_emergency,
            df.loc[scheduled_non_emergency, self.miss_scheduled_visit_probability_column],
        )  # pd.Index
        visitors[data_values.VISIT_TYPE.MISSED] = missed_visit
        df.loc[missed_visit, self.scheduled_visit_date_column] = pd.NaT  # no re-schedule
        visit_scheduled = scheduled_non_emergency.difference(missed_visit)
        visitors[data_values.VISIT_TYPE.SCHEDULED] = visit_scheduled

        # Background visits (for those who did not go for another reason (even missed screenings))
        maybe_background = df.index.difference(visit_emergency.union(visit_scheduled))
        utilization_rate = self.background_utilization_rate(maybe_background)
        visit_background = self.randomness.filter_for_rate(
            maybe_background, utilization_rate
        )  # pd.Index
        visitors[data_values.VISIT_TYPE.BACKGROUND] = visit_background

        df = self.visit_doctor(df, visitors=visitors, event_time=event.time)

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
                    self.sbp_medication_column,
                ]
            ]
        )

    def calculate_medication_coverage_probabilities(self, df: pd.DataFrame) -> Dict:
        """Determine the probability of each simulant being medicated"""
        medication_coverage_covariates = {}
        med_types = data_values.BASELINE_MEDICATION_COVERAGE_COEFFICIENTS._fields
        # TODO: typo in docs - confirm this is correct when clarified
        for med_type in med_types:
            (
                c_int,
                c_sbp,
                c_ldlc,
                c_age,
                c_sex,
            ) = eval(f"data_values.BASELINE_MEDICATION_COVERAGE_COEFFICIENTS.{med_type}")
            medication_coverage_covariates[med_type] = np.exp(
                c_int
                + c_sbp * self.sbp(df.index)
                + c_ldlc * self.ldlc(df.index)
                + c_age * df["age"]
                + c_sex * df["sex"].map(data_values.BASELINE_MEDICATION_COVERAGE_SEX_MAPPING)
            )
        # Calculate probabilities of being medicated
        p_medication = {}
        p_denominator = sum(medication_coverage_covariates.values()) + 1
        for med_type in med_types:
            p_medication[med_type] = pd.Series(
                medication_coverage_covariates[med_type] / p_denominator, name=med_type
            )
        p_medication["NONE"] = pd.Series(1 / p_denominator, name="NONE")

        return p_medication

    def initialize_medication_coverage(self, df: pd.DataFrame):
        """Initializes medication coverage"""
        p_medication = self.calculate_medication_coverage_probabilities(df)
        df_p_meds = pd.concat(p_medication, axis=1)
        medicated_states = self.randomness.choice(
            df_p_meds.index, choices=df_p_meds.columns, p=np.array(df_p_meds)
        )
        medicated_sbp = medicated_states[medicated_states.isin(["SBP", "BOTH"])].index
        medicated_ldlc = medicated_states[medicated_states.isin(["LDLC", "BOTH"])].index

        # Define what level of medication for the medicated simulants
        # TODO: THE BELOW DOES NOT SEEM TO BE PROVIDING EXPECTED RESULTS - CONFIRM IT'S WORKING
        breakpoint()
        df.loc[medicated_sbp, self.sbp_medication_column] = self.randomness.choice(
            medicated_sbp,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY['SBP'].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY['SBP'].values()),
        )
        df.loc[medicated_ldlc, self.ldlc_medication_column] = self.randomness.choice(
            medicated_ldlc,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY['LDLC'].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY['LDLC'].values()),
        )

        # Assign adherence types
        # TODO: THE BELOW DOES NOT SEEM TO BE PROVIDING EXPECTED RESULTS - CONFIRM IT'S WORKING
        breakpoint()
        df.loc[
            medicated_sbp, self.sbp_medication_adherence_type_column
        ] = self.randomness.choice(
            medicated_sbp,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY['SBP'].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY['SBP'].values()),
        )
        df.loc[
            medicated_ldlc, self.ldlc_medication_adherence_type_column
        ] = self.randomness.choice(
            medicated_ldlc,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY['LDLC'].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY['LDLC'].values()),
        )

        return df

    def visit_doctor(
        self,
        df: pd.DataFrame,
        visitors: Dict,
        event_time: pd.Timestamp,
        min_followup: int = data_values.FOLLOWUP_MIN,
        max_followup: int = data_values.FOLLOWUP_MAX,
    ) -> pd.DataFrame:
        """Updates treatment plans and schedules followups.

        Arguments:
            df: state table to update
            visitors: dictionary of all simulants visiting the doctor and their visit types
            event_time: timestamp at end of the time step
            min_followup: minimum number of days out to schedule followup
            max_followup: maximum number of days out to schedule followup
        """
        # Update visit type
        df[self.visit_type_column] = data_values.VISIT_TYPE.NONE  # Reset from last time step
        for k in visitors:
            df.loc[visitors[k], self.visit_type_column] = df[self.visit_type_column] + f";{k}"

        # Update treatments
        to_visit = pd.Index([])
        attended_visit_types = [
            visit_type
            for visit_type in data_values.VISIT_TYPE
            if not visit_type in [data_values.VISIT_TYPE.MISSED, data_values.VISIT_TYPE.NONE]
        ]
        for visit_type in attended_visit_types:
            to_visit = to_visit.union(visitors.get(visit_type, pd.Index([])))

        df.loc[to_visit] = self.update_treatment(df.loc[to_visit])

        # TODO: update based on decision tree
        # Update scheduled visits
        # Only schedule a followup if a future one does not already exist
        breakpoint()
        has_followup_scheduled = df.loc[
            to_visit.intersection(
                df[(df[self.scheduled_visit_date_column] > event_time)].index
            )
        ].index
        to_schedule_followup = to_visit.difference(has_followup_scheduled)

        df.loc[
            to_schedule_followup, self.scheduled_visit_date_column
        ] = self.schedule_followup(
            to_schedule_followup, event_time, min_followup, max_followup
        )

        return df

    def update_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies treatment ramps"""
        df["to_schedule"] = np.nan
        df = self.apply_sbp_treatment_ramp(df)
        df = self.apply_ldlc_treatment_ramp(df)
        return df

    def treat_not_currently_medicated_sbp(self, df: pd.DataFrame) -> pd.DataFrame:
        mid_sbp = df[(df["measured_level"] >= 130) & (df["measured_level"] < 140)].index
        high_sbp = df[df["measured_level"] >= 140].index
        breakpoint()
        
        # Mid-SBP patients
        not_prescribed_mid_sbp = mid_sbp[
            self.randomness.get_draw(mid_sbp) <= data_values.THERAPEUTIC_INERTIA_NO_START
        ]
        to_prescribe_mid_sbp = mid_sbp.difference(not_prescribed_mid_sbp)
        df.loc[not_prescribed_mid_sbp, "to_schedule"] = "yes"
        df.loc[
            to_prescribe_mid_sbp, self.sbp_medication_column
        ] = data_values.SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE
        # TODO: Implement outreach intervention
        df.loc[to_prescribe_mid_sbp, "to_schedule"] = "yes"

        # High-SBP patients
        not_prescribed_high_sbp = high_sbp[
            self.randomness.get_draw(high_sbp) <= data_values.THERAPEUTIC_INERTIA_NO_START
        ]
        to_prescribe_high_sbp = high_sbp.difference(not_prescribed_high_sbp)
        df.loc[not_prescribed_high_sbp, "to_schedule"] = "yes"
        # TODO: is this working? out of 15 draws on first initialization, all get assigned two_drug_half_dose
        df.loc[to_prescribe_high_sbp, self.sbp_medication_column] = self.randomness.choice(
            to_prescribe_high_sbp,
            choices=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY['SBP']['high'].keys()),
            p=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY['SBP']['high'].values()),
        )
        # TODO: implement outreach intervention
        df.loc[to_prescribe_high_sbp, "to_schedule"] = "yes"

        return df

    def treat_currently_medicated_sbp(self, df: pd.DataFrame) -> pd.DataFrame:
        breakpoint()
        return df

    def get_measurement_error(self, index, mean, sd):
        """Return measurement error assuming normal distribution"""
        draw = self.randomness.get_draw(index)
        return scipy.stats.norm(loc=mean, scale=sd).ppf(draw)

    def apply_sbp_treatment_ramp(self, df: pd.DataFrame) -> pd.DataFrame:
        df["measured_level"] = self.sbp(df.index) + self.get_measurement_error(
            df.index,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
        )
        currently_medicated = df[df[self.sbp_medication_column].notna()].index
        not_currently_medicated = df.index.difference(currently_medicated)
        breakpoint()
        df.loc[not_currently_medicated] = self.treat_not_currently_medicated_sbp(
            df.loc[not_currently_medicated]
        )
        breakpoint()
        df.loc[currently_medicated] = self.treat_currently_medicated_sbp(df.loc[currently_medicated])
        breakpoint()
        return df

    def apply_ldlc_treatment_ramp(self, df: pd.DataFrame, index: pd.Index) -> pd.DataFrame:
        # TODO: [MIC-3375]
        return df

    def schedule_followup(
        self,
        index: pd.Index,
        event_time: pd.Timestamp,
        min_followup: int = data_values.FOLLOWUP_MIN,
        max_followup: int = data_values.FOLLOWUP_MAX,
    ) -> pd.Series:
        """Schedules followup visits"""
        return pd.Series(
            event_time
            + self.random_time_delta(
                pd.Series(min_followup, index=index),
                pd.Series(max_followup, index=index),
            ),
            index=index,
        )

    def random_time_delta(self, start: pd.Series, end: pd.Series) -> pd.Series:
        """Generate a random time delta for each individual in the start
        and end series."""
        return pd.to_timedelta(
            start + (end - start) * self.randomness.get_draw(start.index), unit="day"
        )
