import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_values, models
from vivarium_nih_us_cvd.utilities import get_measurement_error


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
        self.randomness = builder.randomness.get_stream(self.name)
        self.sbp = builder.value.get_value(data_values.PIPELINES.SBP_EXPOSURE)
        self.ldlc = builder.value.get_value(data_values.PIPELINES.LDLC_EXPOSURE)

        columns_created = [
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
        ]
        columns_required = [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.MYOCARDIAL_INFARCTION_MODEL_NAME,
        ]

        self.population_view = builder.population.get_view(
            columns_required + columns_created + [data_values.COLUMNS.VISIT_TYPE]
        )

        values_required = [
            data_values.PIPELINES.SBP_EXPOSURE,
            data_values.PIPELINES.LDLC_EXPOSURE,
        ]

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
            requires_columns=columns_required,
            requires_values=values_required,
        )

        # Register listeners
        builder.event.register_listener(
            "time_step__cleanup",
            self.on_time_step_cleanup,
            priority=data_values.TIMESTEP_CLEANUP_PRIORITIES.TREATMENT,
        )

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """Implements baseline medication coverage as well as adherence levels
        Anyone initialized in an emergency (acute) state will enter the treatment
        ramps. A burn-in period allows for the observed simulation to start with
        more realistic treatments and scheduled followups.
        """
        pop = self.population_view.subview(
            [
                "age",
                "sex",
                models.ISCHEMIC_STROKE_MODEL_NAME,
                models.MYOCARDIAL_INFARCTION_MODEL_NAME,
            ]
        ).get(pop_data.index)

        pop = self.initialize_medication_coverage(pop)

        # Send anyone in emergency state to medication ramp
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = pop[(mask_acute_is | mask_acute_mi)]
        pop.loc[mask_emergency] = self.apply_sbp_treatment_ramp(pop_visitors=pop.loc[mask_emergency])
        pop.loc[mask_emergency] = self.apply_ldlc_treatment_ramp(pop_visitors=pop.loc[mask_emergency])

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                ]
            ]
        )

    def initialize_medication_coverage(self, pop: pd.DataFrame) -> pd.DataFrame:
        """Initializes medication coverage and adherence levels"""
        # Initialize adherence levels
        pop[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE] = self.randomness.choice(
            pop.index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["sbp"].values()),
        )
        pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE] = self.randomness.choice(
            pop.index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].values()),
        )

        # Initialize medication coverage
        pop[data_values.COLUMNS.SBP_MEDICATION] = np.nan
        pop[data_values.COLUMNS.LDLC_MEDICATION] = np.nan
        p_medication = self.calculate_initial_medication_coverage_probabilities(pop)
        medicated_states = self.randomness.choice(
            p_medication.index, choices=p_medication.columns, p=np.array(p_medication)
        )
        medicated_sbp = medicated_states[medicated_states.isin(["sbp", "both"])].index
        medicated_ldlc = medicated_states[medicated_states.isin(["ldlc", "both"])].index
        # Define what level of medication for the medicated simulants
        pop.loc[medicated_sbp, data_values.COLUMNS.SBP_MEDICATION] = self.randomness.choice(
            medicated_sbp,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].values()),
        )
        pop.loc[medicated_ldlc, data_values.COLUMNS.LDLC_MEDICATION] = self.randomness.choice(
            medicated_ldlc,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].values()),
        )

        # # Move medicated but non-adherent simulants to lowest level
        sbp_non_adherent = pop[
            pop[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            != data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        ldlc_non_adherent = pop[
            pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE]
            != data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        pop.loc[
            medicated_sbp.intersection(sbp_non_adherent), data_values.COLUMNS.SBP_MEDICATION
        ] = 1
        pop.loc[
            medicated_ldlc.intersection(ldlc_non_adherent),
            data_values.COLUMNS.LDLC_MEDICATION,
        ] = 1

        return pop

    def calculate_initial_medication_coverage_probabilities(self, pop: pd.DataFrame) -> pd.DataFrame:
        """Determine the probability of each simulant being medicated"""

        # Calculate the covariates
        medication_coverage_covariates = {}
        for coefficients in data_values.MEDICATION_COVERAGE_COEFFICIENTS:
            medication_coverage_covariates[coefficients.NAME] = np.exp(
                coefficients.INTERCEPT
                + coefficients.SBP * self.sbp(pop.index)
                + coefficients.LDLC * self.ldlc(pop.index)
                + coefficients.AGE * pop["age"]
                + coefficients.SEX * pop["sex"].map(data_values.BASELINE_MEDICATION_COVERAGE_SEX_MAPPING)
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
        pop = self.population_view.get(event.index, query='alive == "alive"')

        visitors = pop[
            pop[data_values.COLUMNS.VISIT_TYPE].isin(
                [
                    data_values.VISIT_TYPE.EMERGENCY,
                    data_values.VISIT_TYPE.SCHEDULED,
                    data_values.VISIT_TYPE.BACKGROUND,
                ]
            )
        ].index

        pop.loc[visitors] = self.apply_sbp_treatment_ramp(pop_visitors=pop.loc[visitors])
        pop.loc[visitors] = self.apply_ldlc_treatment_ramp(pop_visitors=pop.loc[visitors])

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION,
                ]
            ]
        )

    def apply_sbp_treatment_ramp(self, pop_visitors: pd.DataFrame) -> pd.DataFrame:
        """Applies the SBP treatment ramp

        Arguments:
            pop_visitors: dataframe subset to simulants visiting the doctor
        """
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION].notna()
        ].index
        not_currently_medicated = pop_visitors.index.difference(currently_medicated)
        measured_sbp = self.sbp(pop_visitors.index) + get_measurement_error(
            index=pop_visitors.index,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
            randomness=self.randomness,
        )

        pop_visitors.loc[not_currently_medicated] = self.treat_not_currently_medicated_sbp(
            pop_not_medicated=pop_visitors.loc[not_currently_medicated],
            measured_sbp=measured_sbp,
        )
        pop_visitors.loc[currently_medicated] = self.treat_currently_medicated_sbp(
            pop_medicated=pop_visitors.loc[currently_medicated], measured_sbp=measured_sbp
        )

        return pop_visitors

    def apply_ldlc_treatment_ramp(self, pop_visitors: pd.DataFrame) -> pd.DataFrame:
        # TODO: [MIC-3375]
        return pop_visitors

    def treat_not_currently_medicated_sbp(
        self, pop_not_medicated: pd.DataFrame, measured_sbp: pd.Series
    ) -> pd.DataFrame:
        """Applies the SBP treatment ramp to simulants not already on medication

        Arguments:
            pop_not_medicated: dataframe of simulants subset to those visiting the doctor and
                not already medicated
            measured_sbp: measured blood pressure values
        """

        # Generate indexes for simulants who overcome therapeutic inertia and
        # who have medium and high (measured) SBP levels
        overcome_therapeutic_inertia = pop_not_medicated[
            self.randomness.get_draw(pop_not_medicated.index)
            > data_values.THERAPEUTIC_INERTIA_NO_START
        ].index
        mid_sbp = measured_sbp[
            (measured_sbp >= data_values.SBP_THRESHOLD.LOW)
            & (measured_sbp < data_values.SBP_THRESHOLD.HIGH)
        ].index
        high_sbp = measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.HIGH].index

        # Low-SBP patients: do nothing

        # Mid-SBP patients
        # If tx prescribed, it must be one_drug_half_dose
        to_prescribe_mid_sbp = mid_sbp.intersection(overcome_therapeutic_inertia)
        pop_not_medicated.loc[
            to_prescribe_mid_sbp, data_values.COLUMNS.SBP_MEDICATION
        ] = data_values.MEDICATION_RAMP["sbp"][
            data_values.SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE
        ]

        # High-SBP patients
        # If tx prescribed, apply ramp
        to_prescribe_high_sbp = high_sbp.intersection(overcome_therapeutic_inertia)
        pop_not_medicated.loc[
            to_prescribe_high_sbp, data_values.COLUMNS.SBP_MEDICATION
        ] = self.randomness.choice(
            to_prescribe_high_sbp,
            choices=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].keys()
            ),
            p=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].values()),
        )

        return pop_not_medicated

    def treat_currently_medicated_sbp(
        self, pop_medicated: pd.DataFrame, measured_sbp: pd.Series
    ) -> pd.DataFrame:
        """Applies the SBP treatment ramp to simulants already on medication

        Arguments:
            pop_medicated: dataframe of simulants subset to those visiting the doctor and
                who are already on medication
            measured_sbp: measured blood pressure values
        """

        # Generate indexes for simulants who overcome therapeutic inertia, are
        # adherent to their medication, who are not already medicated, and who
        # have low and high (measured) SBP levels
        high_sbp = measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.HIGH].index
        overcome_therapeutic_inertia = pop_medicated[
            self.randomness.get_draw(pop_medicated.index)
            > data_values.THERAPEUTIC_INERTIA_NO_START
        ].index
        adherent = pop_medicated[
            pop_medicated[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = pop_medicated[
            pop_medicated[data_values.COLUMNS.SBP_MEDICATION]
            < max(data_values.MEDICATION_RAMP["sbp"].values())
        ].index

        # High-SBP patients
        # If tx changed, move up ramp
        medication_change = (
            high_sbp.intersection(overcome_therapeutic_inertia)
            .intersection(adherent)
            .intersection(not_already_max_medicated)
        )
        pop_medicated.loc[medication_change, data_values.COLUMNS.SBP_MEDICATION] = (
            pop_medicated[data_values.COLUMNS.SBP_MEDICATION] + 1
        )

        return pop_medicated
