import numpy as np
import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_values, models
from vivarium_nih_us_cvd.utilities import schedule_followup, get_measurement_error


class Treatment:
    """Updates treatment coverage"""

    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self):
        pass

    def __repr__(self):
        return "Treatment"

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)
        self.sbp = builder.value.get_value("high_systolic_blood_pressure.exposure")
        self.ldlc = builder.value.get_value("high_ldl_cholesterol.exposure")

        # Columns
        self.ischemic_stroke_state_column = models.ISCHEMIC_STROKE_MODEL_NAME
        self.myocardial_infarction_state_column = models.MYOCARDIAL_INFARCTION_MODEL_NAME
        self.visit_type_column = data_values.COLUMNS.VISIT_TYPE
        self.scheduled_visit_date_column = data_values.COLUMNS.SCHEDULED_VISIT_DATE
        self.sbp_medication_column = data_values.COLUMNS.SBP_MEDICATION
        self.ldlc_medication_column = data_values.COLUMNS.LDLC_MEDICATION
        self.sbp_medication_adherence_type_column = (
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE
        )
        self.ldlc_medication_adherence_type_column = (
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE
        )

        columns_created = [
            self.scheduled_visit_date_column,
            self.sbp_medication_column,
            self.ldlc_medication_column,
            self.sbp_medication_adherence_type_column,
            self.ldlc_medication_adherence_type_column,
        ]
        columns_required = [
            "age",
            "sex",
            self.visit_type_column,
            self.ischemic_stroke_state_column,
            self.myocardial_infarction_state_column,
        ]

        self.population_view = builder.population.get_view(columns_required + columns_created)

        values_required = [
            "high_systolic_blood_pressure.exposure",
            "high_ldl_cholesterol.exposure",
        ]

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            requires_columns=columns_required,
            requires_values=values_required,
        )

        # Register listeners
        builder.event.register_listener(
            "time_step__cleanup",
            self.on_time_step_cleanup,
            priority=data_values.COMPONENT_PRIORITIES.TREATMENT,
        )

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """Implements baseline medication coverage as well as adherence levels
        Anyone initialized in an emergency (acute) state will enter the treatment
        ramps.

        This component also initializes scheduled followups because doing so
        requires medication coverage. All simulants on SBP medication, LDL-C
        medication, or a history of an acute event (ie intialized in state
        post-MI or chronic IS) should be initialized with a scheduled followup
        visit 0-6 months out, uniformly distributed. All simulants initialized
        in an acute state should bescheduled a followup visit 3-6 months out,
        uniformly distributed.
        """
        event_time = self.clock() + self.step_size()
        df = self.population_view.subview(
            [
                "age",
                "sex",
                self.visit_type_column,
                self.ischemic_stroke_state_column,
                self.myocardial_infarction_state_column,
            ]
        ).get(pop_data.index)

        # Initialize new columns
        df[self.scheduled_visit_date_column] = pd.NaT
        df[self.sbp_medication_adherence_type_column] = self.randomness.choice(
            df.index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].values()),
        )
        df[self.ldlc_medication_adherence_type_column] = self.randomness.choice(
            df.index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].values()),
        )
        df[self.sbp_medication_column] = np.nan
        df[self.ldlc_medication_column] = np.nan
        df = self.initialize_medication_coverage(df)

        # Schedule followups
        # On medication and/or Post/chronic state 0-6 months out
        on_medication = df[(df[self.sbp_medication_column].notna()) | (df[self.ldlc_medication_column].notna())].index
        mask_chronic_is = (
            df[self.ischemic_stroke_state_column] == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            df[self.myocardial_infarction_state_column]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = df.index[mask_chronic_is | mask_post_mi]
        df.loc[on_medication.union(acute_history), self.scheduled_visit_date_column] = schedule_followup(
            index=acute_history,
            event_time=event_time,
            randomness=self.randomness,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )
        # Emergency (acute) state 3-6 months out
        emergency = df[df[self.visit_type_column] == data_values.VISIT_TYPE.EMERGENCY].index
        df.loc[emergency, self.scheduled_visit_date_column] = schedule_followup(
            index=emergency,
            event_time=event_time,
            randomness=self.randomness,
        )

        # Send anyone in emergency state to medication ramp
        df.loc[emergency] = self.apply_sbp_treatment_ramp(df.loc[emergency])
        df.loc[emergency] = self.apply_ldlc_treatment_ramp(df.loc[emergency])

        self.population_view.update(
            df[
                [
                    self.scheduled_visit_date_column,
                    self.sbp_medication_column,
                    self.sbp_medication_adherence_type_column,
                    self.ldlc_medication_column,
                    self.ldlc_medication_adherence_type_column,
                ]
            ]
        )

    def initialize_medication_coverage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Initializes medication coverage"""
        p_medication = self.calculate_medication_coverage_probabilities(df)
        medicated_states = self.randomness.choice(
            p_medication.index, choices=p_medication.columns, p=np.array(p_medication)
        )
        medicated_sbp = medicated_states[medicated_states.isin(["sbp", "both"])].index
        medicated_ldlc = medicated_states[medicated_states.isin(["ldlc", "both"])].index

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

    def calculate_medication_coverage_probabilities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Determine the probability of each simulant being medicated"""

        # Calculate the covariates
        medication_coverage_covariates = {}
        for (
            name,
            c_int,
            c_sbp,
            c_ldlc,
            c_age,
            c_sex,
        ) in data_values.MEDICATION_COVERAGE_COEFFICIENTS:
            medication_coverage_covariates[name] = np.exp(
                c_int
                + c_sbp * self.sbp(df.index)
                + c_ldlc * self.ldlc(df.index)
                + c_age * df["age"]
                + c_sex * df["sex"].map(data_values.BASELINE_MEDICATION_COVERAGE_SEX_MAPPING)
            )
        # Calculate probabilities of being medicated
        p_medication = {}
        p_denominator = sum(medication_coverage_covariates.values()) + 1
        for med_type, cov in medication_coverage_covariates.items():
            p_medication[med_type] = pd.Series(cov / p_denominator, name=med_type)
        p_medication["none"] = pd.Series(1 / p_denominator, name="none")

        return pd.concat(p_medication, axis=1)

    def on_time_step_cleanup(self, event: Event) -> None:
        """Update treatments"""
        df = self.population_view.subview(
            [
                self.visit_type_column,
                self.sbp_medication_column,
                self.ldlc_medication_column,
                self.sbp_medication_adherence_type_column,
                self.ldlc_medication_adherence_type_column,
            ]
        ).get(event.index, query='alive == "alive"')

        visitors = df[
            df[self.visit_type_column].isin(
                [
                    data_values.VISIT_TYPE.EMERGENCY,
                    data_values.VISIT_TYPE.SCHEDULED,
                    data_values.VISIT_TYPE.BACKGROUND,
                ]
            )
        ].index

        df.loc[visitors] = self.apply_sbp_treatment_ramp(df.loc[visitors])
        df.loc[visitors] = self.apply_ldlc_treatment_ramp(df.loc[visitors])

        self.population_view.update(df[[self.sbp_medication_column, self.ldlc_medication_column,]])

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
            (df["measured_sbp"] >= data_values.SBP_THRESHOLD.LOW)
            & (df["measured_sbp"] < data_values.SBP_THRESHOLD.HIGH)
        ].index
        high_sbp = df[df["measured_sbp"] >= data_values.SBP_THRESHOLD.HIGH].index

        # Low-SBP patients: do nothing

        # Mid-SBP patients
        # If tx prescribed, it must be one_drug_half_dose
        to_prescribe_mid_sbp = mid_sbp.intersection(overcome_therapeutic_inertia)
        df.loc[
            to_prescribe_mid_sbp, self.sbp_medication_column
        ] = data_values.MEDICATION_RAMP["sbp"][
            data_values.SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE
        ]

        # High-SBP patients
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
        high_sbp = df[df["measured_sbp"] >= data_values.SBP_THRESHOLD.HIGH].index
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

        # High-SBP patients
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

    def apply_sbp_treatment_ramp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies the SBP treatment ramp

        Arguments:
            df: dataframe subset to simulants visiting the doctor
        """
        currently_medicated = df[df[self.sbp_medication_column].notna()].index
        not_currently_medicated = df.index.difference(currently_medicated)
        df["measured_sbp"] = self.sbp(
            df.index
        ) + get_measurement_error(
            index=df.index,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
            randomness=self.randomness,
        )

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

    