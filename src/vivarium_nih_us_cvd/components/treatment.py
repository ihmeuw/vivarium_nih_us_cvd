from typing import Callable, Optional

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData
from vivarium.framework.values import Pipeline

from vivarium_nih_us_cvd.constants import data_values, models, paths
from vivarium_nih_us_cvd.utilities import get_random_value_from_normal_distribution

sbp_treatment_map = {
    level.VALUE: level.DESCRIPTION for level in data_values.SBP_MEDICATION_LEVEL
}
ldlc_treatment_map = {
    level.VALUE: level.DESCRIPTION for level in data_values.LDLC_MEDICATION_LEVEL
}


# Format the SBP risk effects file and generate bin edges
sbp_risk_effects = pd.read_csv(paths.FILEPATHS.SBP_MEDICATION_EFFECTS)
sbp_risk_effects.loc[
    sbp_risk_effects["sbp_start_exclusive"].isna(), "sbp_start_exclusive"
] = -float("inf")
sbp_risk_effects.loc[
    sbp_risk_effects["sbp_end_inclusive"].isna(), "sbp_end_inclusive"
] = float("inf")
sbp_bin_edges = sorted(
    set(sbp_risk_effects["sbp_start_exclusive"]).union(
        set(sbp_risk_effects["sbp_end_inclusive"])
    )
)


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
        self.gbd_sbp = builder.value.get_value(data_values.PIPELINES.SBP_GBD_EXPOSURE)
        self.sbp = builder.value.get_value(data_values.PIPELINES.SBP_EXPOSURE)
        self.ldlc = builder.value.get_value(data_values.PIPELINES.LDLC_EXPOSURE)
        self.target_modifier = self._get_target_modifier(builder)
        self._register_target_modifier(builder)

        columns_created = [
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
            data_values.COLUMNS.SBP_MULTIPLIER,
        ]
        columns_required_on_initialization = [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.MYOCARDIAL_INFARCTION_MODEL_NAME,
        ]

        self.population_view = builder.population.get_view(
            columns_required_on_initialization
            + columns_created
            + [data_values.COLUMNS.VISIT_TYPE]
        )

        values_required = [
            data_values.PIPELINES.SBP_GBD_EXPOSURE,
            data_values.PIPELINES.LDLC_EXPOSURE,
        ]

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
            requires_columns=columns_required_on_initialization,
            requires_values=values_required,
            requires_streams=[self.name],
        )

        # Register listeners
        builder.event.register_listener(
            "time_step__cleanup",
            self.on_time_step_cleanup,
            priority=data_values.TIMESTEP_CLEANUP_PRIORITIES.TREATMENT,
        )

    def _get_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        """Apply medication effects"""

        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            """Determine the exposure decrease as treatment_efficacy * adherence_score"""
            pop_view = self.population_view.get(index)
            mask_adherence = (
                pop_view[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
                == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
            )
            df_efficacy = pd.DataFrame(
                {"bin": pd.cut(x=target, bins=sbp_bin_edges, right=True)}
            )
            df_efficacy["sbp_start_exclusive"] = df_efficacy["bin"].apply(lambda x: x.left)
            df_efficacy["sbp_end_inclusive"] = df_efficacy["bin"].apply(lambda x: x.right)
            df_efficacy = pd.concat(
                [df_efficacy, pop_view[data_values.COLUMNS.SBP_MEDICATION]], axis=1
            )
            df_efficacy = (
                df_efficacy.reset_index()
                .merge(
                    sbp_risk_effects,
                    on=[
                        "sbp_start_exclusive",
                        "sbp_end_inclusive",
                        data_values.COLUMNS.SBP_MEDICATION,
                    ],
                    how="left",
                )
                .set_index("index")
            )
            # Simulants not on treatment mean 0 effect
            assert set(
                df_efficacy.loc[
                    df_efficacy["value"].isna(), data_values.COLUMNS.SBP_MEDICATION
                ]
            ) == {data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION}
            df_efficacy.loc[
                df_efficacy[data_values.COLUMNS.SBP_MEDICATION]
                == data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
                "value",
            ] = 0
            assert df_efficacy["value"].isna().sum() == 0
            treatment_efficacy = df_efficacy["value"]

            sbp_decrease = treatment_efficacy * mask_adherence

            return target - sbp_decrease

        return adjust_target

    def _register_target_modifier(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            data_values.PIPELINES.SBP_EXPOSURE,
            modifier=self.target_modifier,
            # requires_values=[f"{self.risk.name}.exposure"],
            requires_columns=[
                data_values.COLUMNS.SBP_MEDICATION,
                data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            ],
        )

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """Implements baseline medication coverage as well as adherence levels
        Anyone initialized in an emergency (acute) state will enter the treatment
        ramps. A burn-in period allows for the observed simulation to start with
        a more realistic treatment spread.
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

        # Generate multiplier columns
        pop[data_values.COLUMNS.SBP_MULTIPLIER] = 1
        mask_sbp_adherent = (
            pop[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        )
        mask_sbp_one_drug = (
            pop[data_values.COLUMNS.SBP_MEDICATION]
            == data_values.SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE.DESCRIPTION
        )
        mask_sbp_two_drugs = (
            pop[data_values.COLUMNS.SBP_MEDICATION]
            == data_values.SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE.DESCRIPTION
        )
        pop.loc[
            (mask_sbp_adherent) & (mask_sbp_one_drug), data_values.COLUMNS.SBP_MULTIPLIER
        ] = data_values.SBP_MULTIPLIER.ONE_DRUG
        pop.loc[
            (mask_sbp_adherent) & (mask_sbp_two_drugs), data_values.COLUMNS.SBP_MULTIPLIER
        ] = data_values.SBP_MULTIPLIER.TWO_DRUGS

        # Send anyone in emergency state to medication ramp
        # Note that for initialization we base the measured exposures on
        # the GBD exposure values
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        pop.loc[mask_emergency] = self.apply_sbp_treatment_ramp(
            pop_visitors=pop.loc[mask_emergency],
            # TODO: Confirm with Syl that this is acceptable to use gbd sbp for measured sbp on initilaization
            exposure_pipeline=self.gbd_sbp,
        )
        pop.loc[mask_emergency] = self.apply_ldlc_treatment_ramp(
            pop_visitors=pop.loc[mask_emergency]
        )

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.SBP_MULTIPLIER,
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
            additional_key="initialize_sbp_adherence",
        )
        pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE] = self.randomness.choice(
            pop.index,
            choices=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].keys()),
            p=list(data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY["ldlc"].values()),
            additional_key="initialize_ldlc_adherence",
        )

        # Initialize medication coverage
        pop[
            data_values.COLUMNS.SBP_MEDICATION
        ] = data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        pop[
            data_values.COLUMNS.LDLC_MEDICATION
        ] = data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        p_medication = self.calculate_initial_medication_coverage_probabilities(pop)
        medicated_states = self.randomness.choice(
            p_medication.index,
            choices=p_medication.columns,
            p=np.array(p_medication),
            additional_key="initial_medication_coverage",
        )
        medicated_sbp = medicated_states[medicated_states.isin(["sbp", "both"])].index
        medicated_ldlc = medicated_states[medicated_states.isin(["ldlc", "both"])].index
        # Define what level of medication for the medicated simulants
        pop.loc[medicated_sbp, data_values.COLUMNS.SBP_MEDICATION] = self.randomness.choice(
            medicated_sbp,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["sbp"].values()),
            additional_key="initial_sbp_medication",
        )
        pop.loc[medicated_ldlc, data_values.COLUMNS.LDLC_MEDICATION] = self.randomness.choice(
            medicated_ldlc,
            choices=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].keys()),
            p=list(data_values.BASELINE_MEDICATION_LEVEL_PROBABILITY["ldlc"].values()),
            additional_key="initial_ldlc_medication",
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
        ] = sbp_treatment_map[1]
        pop.loc[
            medicated_ldlc.intersection(ldlc_non_adherent),
            data_values.COLUMNS.LDLC_MEDICATION,
        ] = ldlc_treatment_map[1]

        return pop

    def calculate_initial_medication_coverage_probabilities(
        self, pop: pd.DataFrame
    ) -> pd.DataFrame:
        """Determine the probability of each simulant being medicated"""

        # Calculate the covariates
        df = pd.DataFrame()
        for coefficients in data_values.MEDICATION_COVERAGE_COEFFICIENTS:
            df[coefficients.NAME] = np.exp(
                coefficients.INTERCEPT
                + coefficients.SBP * self.gbd_sbp(pop.index)
                # TODO: use gbd_ldlc
                + coefficients.LDLC * self.ldlc(pop.index)
                + coefficients.AGE * pop["age"]
                + coefficients.SEX
                * pop["sex"].map(data_values.BASELINE_MEDICATION_COVERAGE_SEX_MAPPING)
            )
        # Calculate probabilities of being medicated
        p_denominator = df.sum(axis=1) + 1
        df = df.divide(p_denominator, axis=0)
        df["none"] = 1 / p_denominator

        return df

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

    def apply_sbp_treatment_ramp(
        self, pop_visitors: pd.DataFrame, exposure_pipeline: Optional[Pipeline] = None
    ) -> pd.DataFrame:
        """Applies the SBP treatment ramp

        Arguments:
            pop_visitors: dataframe subset to simulants visiting the doctor
            exposure_pipeline: the sbp exposure pipeline to use when calculating
                measured sbp values; defaults to the values adjusted for
                population treatment effects in gbd data except during
                initialization
        """

        if not exposure_pipeline:
            exposure_pipeline = self.sbp
        overcome_therapeutic_inertia = pop_visitors[
            self.randomness.get_draw(
                pop_visitors.index,
                additional_key="sbp_therapeutic_inertia",
            )
            > data_values.THERAPEUTIC_INERTIA_NO_START
        ].index
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        ].index
        not_currently_medicated = pop_visitors.index.difference(currently_medicated)

        measured_sbp = self.get_measured_sbp(
            index=pop_visitors.index,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
            exposure_pipeline=exposure_pipeline,
        )

        # Un-medicated patients with sbp >= 140 (who overcome therapeutic inertia)
        # should start medication
        high_sbp = measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.HIGH].index
        to_prescribe_high_sbp = not_currently_medicated.intersection(high_sbp).intersection(
            overcome_therapeutic_inertia
        )
        pop_visitors.loc[
            to_prescribe_high_sbp, data_values.COLUMNS.SBP_MEDICATION
        ] = self.randomness.choice(
            to_prescribe_high_sbp,
            choices=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].keys()
            ),
            p=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].values()),
            additional_key="high_sbp_first_prescriptions",
        )

        # Un-medicated patients with sbp [130, 140) and already-medicated
        # adherent patients with sbp >= 140 should move up the ramp
        # (all must overcome therapeutic inertia in order to move up)
        mid_sbp = measured_sbp[
            (measured_sbp >= data_values.SBP_THRESHOLD.LOW)
            & (measured_sbp < data_values.SBP_THRESHOLD.HIGH)
        ].index
        adherent = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != sbp_treatment_map[max(sbp_treatment_map)]
        ].index
        medication_change = (
            (not_currently_medicated.intersection(mid_sbp))
            .union(
                currently_medicated.intersection(not_already_max_medicated)
                .intersection(adherent)
                .intersection(high_sbp)
            )
            .intersection(overcome_therapeutic_inertia)
        )
        pop_visitors.loc[medication_change, data_values.COLUMNS.SBP_MEDICATION] = (
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION].map(
                {v: k for k, v in sbp_treatment_map.items()}
            )
            + 1
        ).map(sbp_treatment_map)

        return pop_visitors

    def apply_ldlc_treatment_ramp(self, pop_visitors: pd.DataFrame) -> pd.DataFrame:
        # TODO: [MIC-3375]
        return pop_visitors

    def get_measured_sbp(
        self,
        index: pd.Index,
        mean: float,
        sd: float,
        exposure_pipeline: Optional[Pipeline] = None,
    ) -> pd.Series:
        """Introduce a measurement error to the sbp exposure values"""
        if not exposure_pipeline:
            exposure_pipeline = self.sbp
        return exposure_pipeline(index) + get_random_value_from_normal_distribution(
            index=index,
            mean=mean,
            sd=sd,
            randomness=self.randomness,
            additional_key="measured_sbp",
        )

    def get_ascvd(self, visitors: pd.DataFrame) -> pd.Series:
        """Calculate the atherosclerotic cardiovascular disease score"""
        df_visitors = self.population_view.get(visitors)
        return (
            -19.5
            + (0.043 * self.sbp(visitors))
            + (0.266 * df_visitors["age"])
            + (2.32 * df_visitors["sex"].map({"Male": 1, "Female": 0}))
        )

    def get_measured_ldlc(
        self,
        index: pd.Index,
        mean: float,
        sd: float,
        exposure_pipeline: Optional[Pipeline] = None,
    ) -> pd.Series:
        """Introduce a measurement error to the ldlc exposure values"""
        breakpoint()
        if not exposure_pipeline:
            exposure_pipeline = self.ldlc
        return exposure_pipeline(index) + get_random_value_from_normal_distribution(
            index=index,
            mean=mean,
            sd=sd,
            randomness=self.randomness,
            additional_key="measured_ldlc",
        )
