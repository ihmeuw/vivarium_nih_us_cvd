from typing import Dict, Tuple

import numpy as np
import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, data_values, models


ATTENDED_VISIT_TYPES = [visit_type for visit_type in data_values.VISIT_TYPE if not visit_type in [data_values.VISIT_TYPE.MISSED, data_values.VISIT_TYPE.NONE]]


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
        df[self.sbp_medication_column] = np.nan
        df[self.ldlc_medication_column] = np.nan
        df[self.sbp_medication_adherence_type_column] = self.randomness.choice(
            index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].values()),
        )
        df[self.ldlc_medication_adherence_type_column] = self.randomness.choice(
            index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].values()),
        )

        df = self.initialize_medication_coverage(df)

        # Schedule followups for simulants in a post/chronic state
        mask_chronic_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = index[mask_chronic_is | mask_post_mi]
        df.loc[acute_history, self.scheduled_visit_date_column] = self.schedule_followup(
            index=acute_history,
            event_time=event_time,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )

        # Send simulants in an acute state to doctor
        visitors = {}
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        emergency = index[(mask_acute_is | mask_acute_mi)]
        visitors[data_values.VISIT_TYPE.EMERGENCY] = emergency
        df.loc[emergency] = self.visit_doctor(
            df=df.loc[emergency],
            visitors=visitors,
            event_time=event_time,
        )

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
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
        missed_visit = scheduled_non_emergency[self.randomness.get_draw(scheduled_non_emergency) <= data_values.MISS_SCHEDULED_VISIT_PROBABILITY]
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

    def calculate_medication_coverage_probabilities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Determine the probability of each simulant being medicated"""
        medication_coverage_covariates = {}
        med_types = data_values.BASELINE_MEDICATION_COVERAGE_COEFFICIENTS._fields
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

        return pd.concat(p_medication, axis=1)

    def initialize_medication_coverage(self, df: pd.DataFrame):
        """Initializes medication coverage"""
        p_medication = self.calculate_medication_coverage_probabilities(df)
        medicated_states = self.randomness.choice(
            p_medication.index, choices=p_medication.columns, p=np.array(p_medication)
        )
        medicated_sbp = medicated_states[medicated_states.isin(["SBP", "BOTH"])].index
        medicated_ldlc = medicated_states[medicated_states.isin(["LDLC", "BOTH"])].index

        # Define what level of medication for the medicated simulants
        df.loc[medicated_sbp, self.sbp_medication_column] = self.randomness.choice(
            medicated_sbp,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].values()),
        )
        df.loc[medicated_ldlc, self.ldlc_medication_column] = self.randomness.choice(
            medicated_ldlc,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].values()),
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

        # Update treatments (all visit types go through the same treatment ramp)
        to_visit = pd.Index([])
        for visit_type in ATTENDED_VISIT_TYPES:
            to_visit = to_visit.union(visitors.get(visit_type, pd.Index([])))

        df.loc[to_visit], to_schedule_followup = self.update_treatment(df.loc[to_visit])

        # Update scheduled visits (only if a future one does not already exist)
        has_followup_already_scheduled = df[
            (df[self.scheduled_visit_date_column] > event_time)
        ].index
        to_schedule_followup = to_schedule_followup.difference(has_followup_already_scheduled)

        df.loc[
            to_schedule_followup, self.scheduled_visit_date_column
        ] = self.schedule_followup(
            to_schedule_followup, event_time, min_followup, max_followup
        )

        return df

    def update_treatment(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Index]:
        """Applies treatment ramps
        
        Arguments:
            df: state table subset to include only simulants who visit the doctor
        """
        df["to_schedule"] = np.nan  # temporary column
        df = self.apply_sbp_treatment_ramp(df)
        df = self.apply_ldlc_treatment_ramp(df)
        return df, df[df["to_schedule"].notna()].index

    def treat_not_currently_medicated_sbp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies the SBP treatment ramp to simulants not already on medication
        
        Arguments:
            df: dataframe of simulants subset to those visiting the doctor and
                not already medicated
        """

        # Generate indexes for simulants who overcome therapeutic inertia and
        # who have medium and high (measured) SBP levels
        overcome_therapeutic_inertia = df.index[
            self.randomness.get_draw(df.index) > data_values.THERAPEUTIC_INERTIA_NO_START
        ]
        mid_sbp = df[
            (df["measured_level"] >= data_values.SBP_THRESHOLD.LOW)
            & (df["measured_level"] < data_values.SBP_THRESHOLD.HIGH)
        ].index
        high_sbp = df[df["measured_level"] >= data_values.SBP_THRESHOLD.HIGH].index

        # Low-SBP patients: do nothing

        # Mid-SBP patients
        # Everyone gets scheduled a followup
        df.loc[mid_sbp, "to_schedule"] = 1
        # If tx prescribed, it must be one_drug_half_dose
        to_prescribe_mid_sbp = mid_sbp.intersection(overcome_therapeutic_inertia)
        df.loc[
            to_prescribe_mid_sbp, self.sbp_medication_column
        ] = data_values.MEDICATION_RAMP["sbp"][
            data_values.SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE
        ]

        # High-SBP patients
        # Everyone gets scheduled a followup
        df.loc[high_sbp, "to_schedule"] = 1
        # If tx prescribed, apply ramp
        to_prescribe_high_sbp = high_sbp.intersection(overcome_therapeutic_inertia)
        df.loc[to_prescribe_high_sbp, self.sbp_medication_column] = self.randomness.choice(
            to_prescribe_high_sbp,
            choices=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].keys()
            ),
            p=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].values()),
        )

        return df

    def treat_currently_medicated_sbp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies the SBP treatment ramp to simulants already on medication
        
        Arguments:
            df: dataframe of simulants subset to those visiting the doctor and
                who are already on medication
        """

        # Generate indexes for simulants who overcome therapeutic inertia, are
        # adherent to their medication, who are not already medicated, and who
        # have low and high (measured) SBP levels
        low_sbp = df[df["measured_level"] < data_values.SBP_THRESHOLD.HIGH].index
        high_sbp = df[df["measured_level"] >= data_values.SBP_THRESHOLD.HIGH].index
        overcome_therapeutic_inertia = df.index[
            self.randomness.get_draw(df.index) > data_values.THERAPEUTIC_INERTIA_NO_START
        ]
        adherent = df[
            df[self.sbp_medication_adherence_type_column]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = df[
            df[self.sbp_medication_column] < max(data_values.MEDICATION_RAMP["sbp"].values())
        ].index

        # Low-SBP patients - just schedule a followup
        df.loc[low_sbp, "to_schedule"] = 1

        # High-SBP patients
        # Everyone gets schecduled a followup
        df.loc[high_sbp, "to_schedule"] = 1
        # If tx changed, move up ramp
        medication_change = (
            high_sbp.intersection(overcome_therapeutic_inertia)
            .intersection(adherent)
            .intersection(not_already_max_medicated)
        )
        df.loc[medication_change, self.sbp_medication_column] = (
            df[self.sbp_medication_column] + 1
        )

        return df

    def get_measurement_error(self, index, mean, sd):
        """Return measurement error assuming normal distribution"""
        draw = self.randomness.get_draw(index)
        return scipy.stats.norm(loc=mean, scale=sd).ppf(draw)

    def apply_sbp_treatment_ramp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies the SBP treatment ramp
        
        Arguments:
            df: dataframe subset to simulants visiting the doctor
        """
        df["measured_level"] = self.sbp(df.index) + self.get_measurement_error(
            df.index,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
        )  # temporary column
        currently_medicated = df[df[self.sbp_medication_column].notna()].index
        not_currently_medicated = df.index.difference(currently_medicated)

        df.loc[not_currently_medicated] = self.treat_not_currently_medicated_sbp(
            df.loc[not_currently_medicated]
        )
        df.loc[currently_medicated] = self.treat_currently_medicated_sbp(
            df.loc[currently_medicated]
        )

        return df

    def apply_ldlc_treatment_ramp(self, df: pd.DataFrame) -> pd.DataFrame:
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
