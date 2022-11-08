from typing import Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData
from vivarium.framework.values import Pipeline

from vivarium_nih_us_cvd.constants import data_values, models, paths
from vivarium_nih_us_cvd.utilities import get_random_value_from_normal_distribution


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
        self.gbd_ldlc = builder.value.get_value(data_values.PIPELINES.LDLC_GBD_EXPOSURE)
        self.ldlc = builder.value.get_value(data_values.PIPELINES.LDLC_EXPOSURE)
        self.sbp_medication_adherence = builder.value.get_value(
            data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE
        )
        self.ldlc_medication_adherence = builder.value.get_value(
            data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE
        )
        self.outreach = builder.value.get_value(data_values.PIPELINES.OUTREACH_EXPOSURE)

        self.sbp_treatment_map = self._get_sbp_treatment_map()
        self.ldlc_treatment_map = self._get_ldlc_treatment_map()
        self.sbp_medication_effects = self._get_sbp_medication_effects()
        self.sbp_bin_edges = self._get_sbp_bin_edges()
        self.sbp_target_modifier = self._get_sbp_target_modifier(builder)
        self.ldlc_medication_effects = self._get_ldlc_medication_effects(builder)
        self.ldlc_target_modifier = self._get_ldlc_target_modifier(builder)
        self._register_target_modifiers(builder)

        columns_created = [
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
            data_values.COLUMNS.SBP_MULTIPLIER,
            data_values.COLUMNS.LDLC_MULTIPLIER,
            data_values.COLUMNS.OUTREACH,
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
            data_values.PIPELINES.LDLC_GBD_EXPOSURE,
            data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE,
            data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE,
            data_values.PIPELINES.OUTREACH_EXPOSURE,
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

    def _get_sbp_treatment_map(self) -> Dict[int, str]:
        return {level.VALUE: level.DESCRIPTION for level in data_values.SBP_MEDICATION_LEVEL}

    def _get_ldlc_treatment_map(self) -> Dict[int, str]:
        return {level.VALUE: level.DESCRIPTION for level in data_values.LDLC_MEDICATION_LEVEL}

    def _get_sbp_medication_effects(self) -> pd.DataFrame:
        """Load and format the SBP risk effects file"""
        sbp_medication_effects = pd.read_csv(paths.FILEPATHS.SBP_MEDICATION_EFFECTS)
        # Missingness in the bin end means no upper limit
        sbp_medication_effects.loc[
            sbp_medication_effects["sbp_end_inclusive"].isna(), "sbp_end_inclusive"
        ] = float("inf")
        return sbp_medication_effects

    def _get_sbp_bin_edges(self) -> List[Union[int, float]]:
        """Determine the sbp exposure bin edges for mapping to treatment effects"""
        return sorted(
            set(self.sbp_medication_effects["sbp_start_exclusive"]).union(
                set(self.sbp_medication_effects["sbp_end_inclusive"])
            )
        )

    def _get_sbp_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        """Apply sbp medication effects"""

        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            """Determine the (additive) sbp exposure decrease as
            treatment_efficacy * adherence_score
            """
            pop_view = self.population_view.get(index)
            mask_adherence = (
                pop_view[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
                == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
            )
            df_efficacy = pd.DataFrame(
                {"bin": pd.cut(x=target, bins=self.sbp_bin_edges, right=True)}
            )
            df_efficacy["sbp_start_exclusive"] = df_efficacy["bin"].apply(lambda x: x.left)
            df_efficacy["sbp_end_inclusive"] = df_efficacy["bin"].apply(lambda x: x.right)
            df_efficacy = pd.concat(
                [df_efficacy, pop_view[data_values.COLUMNS.SBP_MEDICATION]], axis=1
            )
            df_efficacy = (
                df_efficacy.reset_index()
                .merge(
                    self.sbp_medication_effects,
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

    def _get_ldlc_medication_effects(self, builder: Builder) -> Dict[str, float]:
        """Format the ldlc medication effects data"""
        effects = builder.data.load("risk_factor.high_ldl_cholesterol.medication_effect")
        # convert % to decimal
        effects["value"] = effects["value"] / 100
        return dict(zip(effects[data_values.COLUMNS.LDLC_MEDICATION], effects["value"]))

    def _get_ldlc_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        """Apply ldl-c medication effects"""

        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            """Determine the (multiplicitive) ldl-c exposure decrease as
            treatment_efficacy * adherence_score
            """
            pop_view = self.population_view.get(index)
            mask_adherence = (
                pop_view[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE]
                == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
            )
            treatment_efficacy = pop_view[data_values.COLUMNS.LDLC_MEDICATION].map(
                self.ldlc_medication_effects
            )
            # simulants with no treatment have no efficacy
            no_meds = pop_view[
                pop_view[data_values.COLUMNS.LDLC_MEDICATION]
                == data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
            ].index
            treatment_efficacy.loc[no_meds] = 0
            ldlc_multiplier = 1 - treatment_efficacy * mask_adherence

            return target * ldlc_multiplier

        return adjust_target

    def _register_target_modifiers(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            data_values.PIPELINES.SBP_EXPOSURE,
            modifier=self.sbp_target_modifier,
            requires_columns=[
                data_values.COLUMNS.SBP_MEDICATION,
                data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            ],
        )

        builder.value.register_value_modifier(
            data_values.PIPELINES.LDLC_EXPOSURE,
            modifier=self.ldlc_target_modifier,
            requires_columns=[
                data_values.COLUMNS.LDLC_MEDICATION,
                data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
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

        # Generate initial medication adherence columns and initialize coverage
        pop[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE] = self.sbp_medication_adherence(
            pop.index
        )
        pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE] = self.ldlc_medication_adherence(
            pop.index
        )
        pop = self.initialize_medication_coverage(pop)

        # Generate outreach column
        # NOTE: All outreach scenarios in this simulation start with 0%
        # outreach exposure and so there is no need to update adherence
        # levels at this point.
        pop[data_values.COLUMNS.OUTREACH] = self.outreach(pop.index)

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

        pop[data_values.COLUMNS.LDLC_MULTIPLIER] = 1
        mask_ldlc_adherent = (
            pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        )
        mask_ldlc_low = (
            pop[data_values.COLUMNS.LDLC_MEDICATION]
            == data_values.LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION
        )
        mask_ldlc_med = (
            pop[data_values.COLUMNS.LDLC_MEDICATION]
            == data_values.LDLC_MEDICATION_LEVEL.MED.DESCRIPTION
        )
        mask_ldlc_high = (
            pop[data_values.COLUMNS.LDLC_MEDICATION]
            == data_values.LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION
        )
        pop.loc[
            (mask_ldlc_adherent) & (mask_ldlc_low), data_values.COLUMNS.LDLC_MULTIPLIER
        ] = data_values.LDLC_MULTIPLIER.LOW
        pop.loc[
            (mask_ldlc_adherent) & (mask_ldlc_med), data_values.COLUMNS.LDLC_MULTIPLIER
        ] = data_values.LDLC_MULTIPLIER.MED
        pop.loc[
            (mask_ldlc_adherent) & (mask_ldlc_high), data_values.COLUMNS.LDLC_MULTIPLIER
        ] = data_values.LDLC_MULTIPLIER.HIGH

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
            exposure_pipeline=self.gbd_sbp,
        )
        pop.loc[mask_emergency] = self.apply_ldlc_treatment_ramp(
            pop_visitors=pop.loc[mask_emergency],
            ldlc_pipeline=self.gbd_ldlc,
            sbp_pipeline=self.gbd_sbp,
        )

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.SBP_MULTIPLIER,
                    data_values.COLUMNS.LDLC_MULTIPLIER,
                    data_values.COLUMNS.OUTREACH,
                ]
            ]
        )

    def initialize_medication_coverage(self, pop: pd.DataFrame) -> pd.DataFrame:
        """Initializes medication coverage"""
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
        ] = self.sbp_treatment_map[1]
        pop.loc[
            medicated_ldlc.intersection(ldlc_non_adherent),
            data_values.COLUMNS.LDLC_MEDICATION,
        ] = self.ldlc_treatment_map[1]

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
                + coefficients.LDLC * self.gbd_ldlc(pop.index)
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
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
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
            > data_values.SBP_THERAPEUTIC_INERTIA
        ].index
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        ].index

        measured_sbp = self.get_measured_sbp(
            index=pop_visitors.index,
            exposure_pipeline=exposure_pipeline,
        )

        # Generate other useful helper indexes
        low_sbp = measured_sbp[measured_sbp < data_values.SBP_THRESHOLD.LOW].index
        high_sbp = measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.HIGH].index
        newly_prescribed = overcome_therapeutic_inertia.difference(
            currently_medicated
        ).difference(low_sbp)

        # [Treatment ramp ID C] Simulants who overcome therapeutic inertia, have
        # high SBP, and are not currently medicated
        to_prescribe_c = newly_prescribed.intersection(high_sbp)
        # [Treatment ramp ID B] Simulants who overcome therapeutic inertia, have
        # medium-level SBP, and are not currently medicated
        to_prescribe_b = newly_prescribed.difference(to_prescribe_c)
        # [Treatment ramp ID D] Simulants who overcome therapeutic inertia, have
        # high sbp, and are currently medicated
        to_prescribe_d = overcome_therapeutic_inertia.intersection(
            currently_medicated
        ).intersection(high_sbp)

        # Prescribe initial medications
        pop_visitors.loc[
            to_prescribe_c, data_values.COLUMNS.SBP_MEDICATION
        ] = self.randomness.choice(
            to_prescribe_c,
            choices=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].keys()
            ),
            p=list(data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["high"].values()),
            additional_key="high_sbp_first_prescriptions",
        )

        # Change medications
        # Only move up if currently untreated (treatment ramp ID B) or currently
        # treated (treatment ramp ID D) but adherent and not already at the max
        # ramp level
        adherent = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != self.sbp_treatment_map[max(self.sbp_treatment_map)]
        ].index
        medication_change = to_prescribe_b.union(
            to_prescribe_d.intersection(adherent).intersection(not_already_max_medicated)
        )
        pop_visitors.loc[medication_change, data_values.COLUMNS.SBP_MEDICATION] = (
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION].map(
                {v: k for k, v in self.sbp_treatment_map.items()}
            )
            + 1
        ).map(self.sbp_treatment_map)

        # Apply outreach intervention to groups b, c, and everyone already
        # on medication
        maybe_enroll = to_prescribe_b.union(to_prescribe_c).union(currently_medicated)
        pop_visitors = self.enroll_in_outreach(pop_visitors, maybe_enroll)

        return pop_visitors

    def apply_ldlc_treatment_ramp(
        self,
        pop_visitors: pd.DataFrame,
        ldlc_pipeline: Optional[Pipeline] = None,
        sbp_pipeline: Optional[Pipeline] = None,
    ) -> pd.DataFrame:
        """Applies the LDL-C treatment ramp

        Arguments:
            pop_visitors: dataframe subset to simulants visiting the doctor
            ldlc_pipeline: the ldl-c exposure pipeline to use when calculating
                measured ldl-c values; defaults to the values adjusted for
                population treatment effects in gbd data except during
                initialization
            sbp_pipeline: the sbp exposure pipeline to use when calculating
                ASCVD; defaults to the values adjusted for population treatment
                effects in gbd data except during initialization
        """
        if not ldlc_pipeline:
            ldlc_pipeline = self.ldlc
        if not sbp_pipeline:
            sbp_pipeline = self.sbp
        overcome_therapeutic_inertia = pop_visitors[
            self.randomness.get_draw(
                pop_visitors.index,
                additional_key="ldlc_therapeutic_inertia",
            )
            > data_values.LDLC_THERAPEUTIC_INERTIA
        ].index
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.LDLC_MEDICATION]
            != data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        ].index

        ascvd = self.get_ascvd(pop_visitors=pop_visitors, sbp_pipeline=sbp_pipeline)
        measured_ldlc = self.get_measured_ldlc(
            index=pop_visitors.index,
            exposure_pipeline=ldlc_pipeline,
        )

        # Generate other useful helper indexes
        low_ascvd = ascvd[ascvd < data_values.ASCVD_THRESHOLD.LOW].index
        high_ascvd = ascvd[ascvd >= data_values.ASCVD_THRESHOLD.HIGH].index
        low_ldlc = measured_ldlc[measured_ldlc < data_values.LDLC_THRESHOLD.LOW].index
        high_ldlc = measured_ldlc[measured_ldlc >= data_values.LDLC_THRESHOLD.HIGH].index
        mask_history_mi = (
            pop_visitors[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            != models.MYOCARDIAL_INFARCTION_SUSCEPTIBLE_STATE_NAME
        )
        mask_history_is = (
            pop_visitors[models.ISCHEMIC_STROKE_MODEL_NAME]
            != models.ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME
        )
        history_mi_or_is = pop_visitors[mask_history_mi | mask_history_is].index
        newly_prescribed = (
            overcome_therapeutic_inertia.difference(currently_medicated)
            .difference(low_ascvd)
            .difference(low_ldlc)
        )

        # [Treatment ramp ID D] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, and
        # have a history of MI or IS
        to_prescribe_d = newly_prescribed.intersection(history_mi_or_is)
        # [Treatment ramp ID E] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, have
        # no history of MI or IS, and who have high LDLC or ASCVD
        to_prescribe_e = newly_prescribed.difference(to_prescribe_d).intersection(
            high_ascvd.union(high_ldlc)
        )
        # [Treatment ramp ID F] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, have
        # no history of MI or IS, but who do NOT have high LDLC or ASCVD
        to_prescribe_f = newly_prescribed.difference(to_prescribe_d).difference(
            to_prescribe_e
        )
        # [Treatment ramp ID G] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, and are currently medicated
        to_prescribe_g = overcome_therapeutic_inertia.intersection(
            currently_medicated
        ).difference(low_ldlc)

        # Prescribe initial medications
        df_newly_prescribed = pd.DataFrame(index=newly_prescribed)
        df_newly_prescribed.loc[
            to_prescribe_d,
            data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_d"].keys(),
        ] = data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_d"].values()
        df_newly_prescribed.loc[
            to_prescribe_e,
            data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_e"].keys(),
        ] = data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_e"].values()
        df_newly_prescribed.loc[
            to_prescribe_f,
            data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_f"].keys(),
        ] = data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["ldlc"]["ramp_id_f"].values()
        pop_visitors.loc[
            newly_prescribed, data_values.COLUMNS.LDLC_MEDICATION
        ] = self.randomness.choice(
            newly_prescribed,
            choices=df_newly_prescribed.columns,
            p=np.array(df_newly_prescribed),
            additional_key="high_ldlc_first_prescriptions",
        )

        # Change medications
        # Only move up if adherent and not already at the max ramp level
        adherent = pop_visitors[
            pop_visitors[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.LDLC_MEDICATION]
            != self.ldlc_treatment_map[max(self.ldlc_treatment_map)]
        ].index
        medication_change = to_prescribe_g.intersection(adherent).intersection(
            not_already_max_medicated
        )
        pop_visitors.loc[medication_change, data_values.COLUMNS.LDLC_MEDICATION] = (
            pop_visitors[data_values.COLUMNS.LDLC_MEDICATION].map(
                {v: k for k, v in self.ldlc_treatment_map.items()}
            )
            + 1
        ).map(self.ldlc_treatment_map)

        # Apply outreach intervention to groups 'newly_prescribed' (d, e, f)
        # and simulants already on medication
        maybe_enroll = newly_prescribed.union(currently_medicated)
        pop_visitors = self.enroll_in_outreach(pop_visitors, maybe_enroll)

        return pop_visitors

    def enroll_in_outreach(
        self, pop_visitors: pd.DataFrame, maybe_enroll: pd.Index
    ) -> pd.DataFrame:
        """Enrolls simulants in outreach intervention programs. It updates
        the outreach column as well as both medication adherence columns (the
        effect of outreach intervention).

        Reminder: 'cat1' outreach means enrolled and 'cat2' means not enrolled.
        """
        current_outreach = pop_visitors.loc[maybe_enroll, data_values.COLUMNS.OUTREACH]
        new_outreach = self.outreach(maybe_enroll)
        to_enroll = current_outreach[current_outreach != new_outreach].index
        if not to_enroll.empty:
            breakpoint()  # save for next PR
            # Update the outreach column with the new pipeline values. This
            # is then used in OutreachEffect which registers a value modifier
            # using the (newly updated) outreach column to modify adherence exposure.
            pop_visitors.loc[to_enroll, data_values.COLUMNS.OUTREACH] = new_outreach.loc[
                to_enroll
            ]
            self.population_view.update(pop_visitors[[data_values.COLUMNS.OUTREACH]])
            # With the just-updated outreach column, update the medication
            # adherence columns with pipeline values
            pop_visitors.loc[
                to_enroll,
                [
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                ],
            ] = [
                self.sbp_medication_adherence(to_enroll),
                self.ldlc_medication_adherence(to_enroll),
            ]
            self.population_view.update(
                pop_visitors[
                    [
                        data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                        data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                    ]
                ]
            )

        return pop_visitors

    def get_measured_sbp(
        self,
        index: pd.Index,
        exposure_pipeline: Optional[Pipeline] = None,
    ) -> pd.Series:
        """Introduce a measurement error to the sbp exposure values"""
        if not exposure_pipeline:
            exposure_pipeline = self.sbp

        return (
            exposure_pipeline(index)
            + get_random_value_from_normal_distribution(
                index=index,
                mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
                sd=data_values.MEASUREMENT_ERROR_SD_SBP,
                randomness=self.randomness,
                additional_key="measured_sbp",
            )
        ).clip(lower=0)

    def get_ascvd(
        self, pop_visitors: pd.DataFrame, sbp_pipeline: Optional[Pipeline] = None
    ) -> pd.Series:
        """Calculate the atherosclerotic cardiovascular disease score"""
        if not sbp_pipeline:
            sbp_pipeline = self.sbp

        return (
            data_values.ASCVD_COEFFICIENTS.INTERCEPT
            + (data_values.ASCVD_COEFFICIENTS.SBP * sbp_pipeline(pop_visitors.index))
            + (data_values.ASCVD_COEFFICIENTS.AGE * pop_visitors["age"])
            + (
                data_values.ASCVD_COEFFICIENTS.SEX
                * pop_visitors["sex"].map(data_values.ASCVD_SEX_MAPPING)
            )
        )

    def get_measured_ldlc(
        self,
        index: pd.Index,
        exposure_pipeline: Optional[Pipeline] = None,
    ) -> pd.Series:
        """Introduce a measurement error to the ldlc exposure values"""
        if not exposure_pipeline:
            exposure_pipeline = self.ldlc

        return (
            exposure_pipeline(index)
            + get_random_value_from_normal_distribution(
                index=index,
                mean=data_values.MEASUREMENT_ERROR_MEAN_LDLC,
                sd=data_values.MEASUREMENT_ERROR_SD_LDLC,
                randomness=self.randomness,
                additional_key="measured_ldlc",
            )
        ).clip(lower=0)
