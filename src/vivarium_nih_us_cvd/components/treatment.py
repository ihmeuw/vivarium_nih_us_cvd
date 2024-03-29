from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from scipy import stats
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.lookup import LookupTable
from vivarium.framework.population import SimulantData
from vivarium.framework.utilities import probability_to_rate
from vivarium.framework.values import Pipeline

from vivarium_nih_us_cvd.constants import (
    data_keys,
    data_values,
    models,
    paths,
    scenarios,
)
from vivarium_nih_us_cvd.utilities import get_random_value_from_normal_distribution


class Treatment(Component):
    """Updates treatment coverage"""

    ##############
    # Properties #
    ##############

    @property
    def columns_created(self) -> List[str]:
        return [
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
            data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
            data_values.COLUMNS.SBP_MEDICATION_START_DATE,
            data_values.COLUMNS.LDLC_MEDICATION_START_DATE,
            data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION,
            data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION,
            data_values.COLUMNS.LIFESTYLE_ADHERENCE,
            data_values.COLUMNS.SBP_MULTIPLIER,
            data_values.COLUMNS.LDLC_MULTIPLIER,
            data_values.COLUMNS.OUTREACH,
            data_values.COLUMNS.POLYPILL,
            data_values.COLUMNS.LIFESTYLE,
            data_values.COLUMNS.SBP_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
            data_values.COLUMNS.LDLC_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
        ]

    @property
    def columns_required(self) -> Optional[List[str]]:
        return [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
            data_values.COLUMNS.VISIT_TYPE,
            data_values.COLUMNS.LAST_FPG_TEST_DATE,
            "tracked",
        ]

    @property
    def initialization_requirements(self) -> Dict[str, List[str]]:
        return {
            "requires_columns": [
                "age",
                "sex",
                models.ISCHEMIC_STROKE_MODEL_NAME,
                models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
                "high_systolic_blood_pressure_propensity",
            ],
            "requires_values": [
                data_values.PIPELINES.SBP_GBD_EXPOSURE,
                data_values.PIPELINES.LDLC_GBD_EXPOSURE,
                data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE,
                data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE,
                data_values.PIPELINES.OUTREACH_EXPOSURE,
                data_values.PIPELINES.POLYPILL_EXPOSURE,
                data_values.PIPELINES.LIFESTYLE_EXPOSURE,
                data_values.PIPELINES.BMI_EXPOSURE,
                data_values.PIPELINES.FPG_EXPOSURE,
            ],
            "requires_streams": [self.name],
        }

    @property
    def time_step_cleanup_priority(self) -> int:
        return data_values.TIMESTEP_CLEANUP_PRIORITIES.TREATMENT

    #####################
    # Lifecycle methods #
    #####################

    def setup(self, builder: Builder) -> None:
        self.randomness = builder.randomness.get_stream(self.name)
        self.scenario = self._get_scenario(builder)
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()

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
        self.polypill = builder.value.get_value(data_values.PIPELINES.POLYPILL_EXPOSURE)
        self.lifestyle = builder.value.get_value(data_values.PIPELINES.LIFESTYLE_EXPOSURE)
        self.bmi = builder.value.get_value(data_values.PIPELINES.BMI_EXPOSURE)
        self.bmi_raw = builder.value.get_value(data_values.PIPELINES.BMI_RAW_EXPOSURE)
        self.fpg = builder.value.get_value(data_values.PIPELINES.FPG_EXPOSURE)

        self.sbp_treatment_map = self._get_sbp_treatment_map()
        self.ldlc_treatment_map = self._get_ldlc_treatment_map()
        self.sbp_medication_effects = self._get_sbp_medication_effects()
        self.sbp_bin_edges = self._get_sbp_bin_edges()
        self.sbp_target_modifier = self._get_sbp_target_modifier(builder)
        self.ldlc_medication_effects = self._get_ldlc_medication_effects(builder)
        self.ldlc_target_modifier = self._get_ldlc_target_modifier(builder)
        self.medication_coverage_scaling_factors = (
            self._get_medication_coverage_scaling_factors(builder)
        )
        self._register_target_modifiers(builder)

    #################
    # Setup methods #
    #################

    def _get_scenario(self, builder: Builder) -> scenarios.InterventionScenario:
        return scenarios.INTERVENTION_SCENARIOS[builder.configuration.intervention.scenario]

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

            # Assign untracked people to no medication before concating to efficacy so asserts pass
            pop_view.loc[
                ~pop_view["tracked"], data_values.COLUMNS.SBP_MEDICATION
            ] = data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION

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
            ).issubset({data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION})
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

    def _get_medication_coverage_scaling_factors(self, builder: Builder) -> LookupTable:
        sf = builder.data.load(data_keys.MEDICATION_COVERAGE.SCALING_FACTOR)
        return builder.lookup.build_table(sf, parameter_columns=["age"], key_columns=["sex"])

    def _register_target_modifiers(self, builder: Builder) -> None:
        # medication effects
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

        # drop value modifiers
        builder.value.register_value_modifier(
            data_values.PIPELINES.BMI_DROP_VALUE,
            modifier=self._apply_lifestyle_to_bmi,
            requires_columns=[
                data_values.COLUMNS.LIFESTYLE,
            ],
        )

        builder.value.register_value_modifier(
            data_values.PIPELINES.FPG_DROP_VALUE,
            modifier=self._apply_lifestyle_to_fpg,
            requires_columns=[
                data_values.COLUMNS.LIFESTYLE,
            ],
        )

        builder.value.register_value_modifier(
            data_values.PIPELINES.SBP_DROP_VALUE,
            modifier=self._apply_lifestyle_to_sbp,
            requires_columns=[
                data_values.COLUMNS.LIFESTYLE,
            ],
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def _apply_lifestyle(self, index: pd.Index, target: pd.Series, risk: str):
        # allow for updating drop value of dead people - makes interacting with target easier
        pop = self.population_view.get(index)
        enrollment_dates = pop[data_values.COLUMNS.LIFESTYLE]
        updated_drop_values = self.get_updated_drop_values(
            target.copy(), enrollment_dates, risk=risk
        )

        return updated_drop_values

    def _apply_lifestyle_to_bmi(self, index, target):
        return self._apply_lifestyle(index, target, risk="bmi")

    def _apply_lifestyle_to_fpg(self, index, target):
        return self._apply_lifestyle(index, target, risk="fpg")

    def _apply_lifestyle_to_sbp(self, index, target):
        return self._apply_lifestyle(index, target, risk="sbp")

    ########################
    # Event-driven methods #
    ########################

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
                models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
            ]
        ).get(pop_data.index)

        # Define therapeutic inertia constant components
        pop[
            data_values.COLUMNS.SBP_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT
        ] = get_random_value_from_normal_distribution(
            index=pop.index,
            mean=0.0,
            sd=np.sqrt(data_values.THERAPEUTIC_INERTIA_CONSTANT_COMPONENT_VARIANCE),
            randomness=self.randomness,
            additional_key=data_values.COLUMNS.SBP_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
        )
        pop[
            data_values.COLUMNS.LDLC_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT
        ] = get_random_value_from_normal_distribution(
            index=pop.index,
            mean=0.0,
            sd=np.sqrt(data_values.THERAPEUTIC_INERTIA_CONSTANT_COMPONENT_VARIANCE),
            randomness=self.randomness,
            additional_key=data_values.COLUMNS.LDLC_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
        )

        # Generate initial medication adherence columns and initialize coverage
        pop[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE] = self.sbp_medication_adherence(
            pop.index
        )
        pop[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE] = self.ldlc_medication_adherence(
            pop.index
        )
        pop = self.initialize_medication_coverage(pop)
        ## NOTE: These two methods modify the pop dataframe
        self.initialize_medication_discontinuation(
            pop=pop,
            medication_col=data_values.COLUMNS.SBP_MEDICATION,
            start_date_col=data_values.COLUMNS.SBP_MEDICATION_START_DATE,
            discontinued_col=data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION,
            no_treatment_description=data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
        )
        self.initialize_medication_discontinuation(
            pop=pop,
            medication_col=data_values.COLUMNS.LDLC_MEDICATION,
            start_date_col=data_values.COLUMNS.LDLC_MEDICATION_START_DATE,
            discontinued_col=data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION,
            no_treatment_description=data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
        )

        # Generate lifestyle adherence column
        lifestyle_propensity = self.randomness.get_draw(
            pop_data.index, additional_key="lifestyle_propensity"
        )
        pop[data_values.COLUMNS.LIFESTYLE_ADHERENCE] = (
            lifestyle_propensity > data_values.LIFESTYLE_DROP_VALUES.PERCENTAGE_NON_ADHERENT
        )

        # Generate outreach, polypill, and lifestyle intervention columns
        # NOTE: All scenarios in this simulation start with 0%
        # intervention exposure for outreach and polypill so there
        # is no need to update adherence levels at this point.
        pop[data_values.COLUMNS.OUTREACH] = self.outreach(pop.index)
        pop[data_values.COLUMNS.POLYPILL] = self.polypill(pop.index)
        pop[data_values.COLUMNS.LIFESTYLE] = pd.NaT

        # Generate column for last FPG test date
        bmi = self.bmi_raw(pop.index)
        age = pop["age"]
        is_eligible_for_testing = (
            age >= data_values.FPG_TESTING.AGE_ELIGIBILITY_THRESHOLD
        ) & (bmi >= data_values.FPG_TESTING.BMI_ELIGIBILITY_THRESHOLD)

        # Determine which eligible simulants get assigned a test date
        simulants_with_test_date = self.randomness.filter_for_probability(
            pop[is_eligible_for_testing],
            [data_values.FPG_TESTING.PROBABILITY_OF_TESTING_GIVEN_ELIGIBLE]
            * sum(is_eligible_for_testing),
        )

        # sample from dates uniformly distributed from 0 to 3 years before sim start date
        draws = self.randomness.get_draw(
            index=simulants_with_test_date.index, additional_key="fpg_test_date"
        )
        time_before_event_start = draws * pd.Timedelta(
            days=365.25 * data_values.FPG_TESTING.MIN_YEARS_BETWEEN_TESTS
        )

        fpg_test_date_column = pd.Series(pd.NaT, index=pop.index)
        fpg_test_date_column[simulants_with_test_date.index] = (
            self.clock() + self.step_size() - time_before_event_start
        )
        pop[data_values.COLUMNS.LAST_FPG_TEST_DATE] = fpg_test_date_column

        # Generate multiplier columns
        pop[data_values.COLUMNS.SBP_MULTIPLIER] = 1.0
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

        pop[data_values.COLUMNS.LDLC_MULTIPLIER] = 1.0
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
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        # NOTE: during initialization we base the measured exposures on
        # the GBD exposure values
        # NOTE: we do not do anything with interventions durint initialization
        # because the simulation always starts at 0% rampup
        pop.loc[mask_emergency], _ = self.apply_sbp_treatment_ramp(
            pop_visitors=pop.loc[mask_emergency].copy(),
            exposure_pipeline=self.gbd_sbp,
        )
        pop.loc[mask_emergency], _ = self.apply_ldlc_treatment_ramp(
            pop_visitors=pop.loc[mask_emergency].copy(),
            ldlc_pipeline=self.gbd_ldlc,
            sbp_pipeline=self.gbd_sbp,
        )

        # We update the medication adherence columns and the outreach column here
        # because self.enroll_in_outreach does not update these during initialization
        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.SBP_MEDICATION_START_DATE,
                    data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.LDLC_MEDICATION_START_DATE,
                    data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION,
                    data_values.COLUMNS.LIFESTYLE_ADHERENCE,
                    data_values.COLUMNS.SBP_MULTIPLIER,
                    data_values.COLUMNS.LDLC_MULTIPLIER,
                    data_values.COLUMNS.OUTREACH,
                    data_values.COLUMNS.POLYPILL,
                    data_values.COLUMNS.LIFESTYLE,
                    data_values.COLUMNS.SBP_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
                    data_values.COLUMNS.LDLC_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT,
                ]
            ]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Update treatments"""
        pop = self.population_view.get(event.index, query='alive == "alive" & tracked==True')

        # Discontinue medications
        ## NOTE: These two methods modify the pop dataframe
        self.discontinue_medication(
            pop=pop,
            event=event,
            medication_col=data_values.COLUMNS.SBP_MEDICATION,
            start_date_col=data_values.COLUMNS.SBP_MEDICATION_START_DATE,
            discontinued_col=data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION,
            no_treatment_description=data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
        )
        self.discontinue_medication(
            pop=pop,
            event=event,
            medication_col=data_values.COLUMNS.LDLC_MEDICATION,
            start_date_col=data_values.COLUMNS.LDLC_MEDICATION_START_DATE,
            discontinued_col=data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION,
            no_treatment_description=data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
        )

        # Apply treatment ramps (i.e. visit the hospital)
        visitors = pop[
            pop[data_values.COLUMNS.VISIT_TYPE].isin(
                [
                    data_values.VISIT_TYPE.EMERGENCY,
                    data_values.VISIT_TYPE.SCHEDULED,
                    data_values.VISIT_TYPE.BACKGROUND,
                ]
            )
        ].index

        pop.loc[visitors], maybe_enroll_sbp = self.apply_sbp_treatment_ramp(
            pop_visitors=pop.loc[visitors]
        )
        pop.loc[visitors], maybe_enroll_ldlc = self.apply_ldlc_treatment_ramp(
            pop_visitors=pop.loc[visitors]
        )

        # Enroll in interventions. The sbp treatment ramp includes both
        # outreach and polypill enrollment logic while the ldlc ramp
        # only includes outreach
        if self.scenario.is_outreach_scenario:
            maybe_enroll = maybe_enroll_sbp.union(maybe_enroll_ldlc)
            pop.loc[visitors] = self.enroll_in_outreach(
                pop_visitors=pop.loc[visitors], maybe_enroll=maybe_enroll
            )
        if self.scenario.is_polypill_scenario:
            pop.loc[visitors] = self.enroll_in_polypill(
                pop_visitors=pop.loc[visitors], maybe_enroll=maybe_enroll_sbp
            )

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.SBP_MEDICATION,
                    data_values.COLUMNS.SBP_MEDICATION_START_DATE,
                    data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION,
                    data_values.COLUMNS.LDLC_MEDICATION_START_DATE,
                    data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION,
                ]
            ]
        )

    ##################
    # Helper methods #
    ##################

    def get_updated_drop_values(self, target, enrollment_dates, risk):
        try:
            initial_drop_value, final_drop_value = {
                "bmi": (
                    data_values.LIFESTYLE_DROP_VALUES.BMI_INITIAL_DROP_VALUE,
                    data_values.LIFESTYLE_DROP_VALUES.BMI_FINAL_DROP_VALUE,
                ),
                "fpg": (
                    data_values.LIFESTYLE_DROP_VALUES.FPG_INITIAL_DROP_VALUE,
                    data_values.LIFESTYLE_DROP_VALUES.FPG_FINAL_DROP_VALUE,
                ),
                "sbp": (
                    data_values.LIFESTYLE_DROP_VALUES.SBP_INITIAL_DROP_VALUE,
                    data_values.LIFESTYLE_DROP_VALUES.SBP_FINAL_DROP_VALUE,
                ),
            }[risk]
        except KeyError:
            raise ValueError(f"Unrecognized risk {risk}. Risk should be bmi, fpg, or sbp.")

        # drop value at enrollment and during maintenance period
        decreasing_period_start_dates = enrollment_dates + pd.Timedelta(
            days=365.25 * data_values.LIFESTYLE_DROP_VALUES.YEARS_IN_MAINTENANCE_PERIOD
        )
        at_initial_drop_value = self.clock() <= decreasing_period_start_dates
        target.loc[at_initial_drop_value] = initial_drop_value

        # update drop value for decreasing period
        decreasing_period_end_dates = decreasing_period_start_dates + pd.Timedelta(
            days=365.25 * data_values.LIFESTYLE_DROP_VALUES.YEARS_IN_DECREASING_PERIOD
        )
        progress = (self.clock() - decreasing_period_start_dates) / (
            pd.Timedelta(
                days=365.25 * data_values.LIFESTYLE_DROP_VALUES.YEARS_IN_DECREASING_PERIOD
            )
        )
        in_decreasing_period = (decreasing_period_start_dates < self.clock()) & (
            self.clock() <= decreasing_period_end_dates
        )
        target.loc[in_decreasing_period] = initial_drop_value - progress[
            in_decreasing_period
        ] * (initial_drop_value - final_drop_value)

        # update drop value once final value has been reached
        reached_final_value = self.clock() > decreasing_period_end_dates
        target.loc[reached_final_value] = final_drop_value

        # don't update drop values for non-adherent simulants
        target = (
            target
            * self.population_view.get(target.index)[data_values.COLUMNS.LIFESTYLE_ADHERENCE]
        )

        return target

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

        # Move medicated but non-adherent simulants to lowest level
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
        # Apply covariate scaling factor "relative risks"
        sf = self.medication_coverage_scaling_factors(df.index)
        df["sbp"] *= sf["sbp_rr"]
        df["ldlc"] *= sf["ldl_rr"]
        df["both"] *= sf["both_rr"]

        # Calculate probabilities of being medicated
        p_denominator = df.sum(axis=1) + 1
        df = df.divide(p_denominator, axis=0)
        df["none"] = 1 / p_denominator

        return df

    def initialize_medication_discontinuation(
        self,
        pop: pd.DataFrame,
        medication_col: str,
        start_date_col: str,
        discontinued_col: str,
        no_treatment_description: str,
    ) -> None:
        """Initializes medication discontinuation"""

        pop[start_date_col] = pd.NaT
        pop[discontinued_col] = False

        sim_start = self.clock() + self.step_size()

        # Uniformly distribute medication start dates between 0-3 years in the past
        medicated_idx = pop[pop[medication_col] != no_treatment_description].index
        pop.loc[
            medicated_idx,
            start_date_col,
        ] = sim_start - self.randomness.get_draw(
            index=medicated_idx,
            additional_key=f"initialize_{start_date_col}",
        ) * pd.Timedelta(
            days=365.25 * data_values.INITIALIZATION_MEDICATON_START_DATE_YEARS_IN_PAST
        )

        # Initialize medication discontinuation
        not_medicated_idx = pop.index.difference(medicated_idx)
        # NOTE: The raw discontinuation "relative risk" values were scaled by RT such
        # that the scaled probabilities that get calculated in the Treatment component
        # are <= ~0.95. Ensure future data updates guarantee this as well or
        # the issue is otherwise handled.
        scaling_factor = self.medication_coverage_scaling_factors(not_medicated_idx)[
            {
                data_values.COLUMNS.SBP_MEDICATION: "sbp_rr",
                data_values.COLUMNS.LDLC_MEDICATION: "ldl_rr",
            }[medication_col]
        ]
        probs = data_values.MEDICATION_DISCONTINUATION_PROBABILITY / scaling_factor
        discontinued_idx = self.randomness.filter_for_probability(
            population=not_medicated_idx,
            probability=probs,
            additional_key=f"initialize_{discontinued_col}",
        )
        pop.loc[discontinued_idx, discontinued_col] = True

    def discontinue_medication(
        self,
        pop: pd.DataFrame,
        event: Event,
        medication_col: str,
        start_date_col: str,
        discontinued_col: str,
        no_treatment_description: str,
    ) -> None:
        """Simulants who started medication within the last year have a chance of discontinuing.
        If chosen, set the discontinued column to True and the medication column to not medicated.
        """
        treated_idx = pop[pop[medication_col] != no_treatment_description].index
        started_recently_idx = pop[
            pop[start_date_col] >= event.time - pd.Timedelta(days=365.25)
        ].index
        maybe_discontinue_idx = treated_idx.intersection(started_recently_idx)

        # The probability of discontinuing is per year so we can convert that to a rate and scale
        # NOTE: The raw discontinuation "relative risk" values were scaled by RT such
        # that the scaled probabilities that get calculated in the Treatment component
        # are <= ~0.95. Ensure future data updates guarantee this as well or
        # the issue is otherwise handled.
        scaling_factor = self.medication_coverage_scaling_factors(maybe_discontinue_idx)[
            {
                data_values.COLUMNS.SBP_MEDICATION: "sbp_rr",
                data_values.COLUMNS.LDLC_MEDICATION: "ldl_rr",
            }[medication_col]
        ]
        probs = data_values.MEDICATION_DISCONTINUATION_PROBABILITY / scaling_factor
        scaled_rates = probability_to_rate(probs) * self.step_size().days / 365.25

        discontinue_idx = self.randomness.filter_for_rate(
            population=maybe_discontinue_idx,
            rate=scaled_rates,
            additional_key=f"discontinue_{medication_col}",
        )
        pop.loc[discontinue_idx, discontinued_col] = True
        pop.loc[discontinue_idx, medication_col] = no_treatment_description

    def apply_sbp_treatment_ramp(
        self, pop_visitors: pd.DataFrame, exposure_pipeline: Optional[Pipeline] = None
    ) -> Tuple[pd.DataFrame, pd.Index]:
        """Applies the SBP treatment ramp.

        NOTE: simulants who have discontinued medication are not candidates
        for outreach or polypill interventions.
        NOTE: simulants who have discontinued medication are not candidates
        for being put (back on) medication.

        Arguments:
            pop_visitors: dataframe subset to simulants visiting the doctor
            exposure_pipeline: the sbp exposure pipeline to use when calculating
                measured sbp values; defaults to the values adjusted for
                population treatment effects in gbd data except during
                initialization
        """
        if not exposure_pipeline:
            exposure_pipeline = self.sbp

        sbp_therapeutic_inertia_dynamic_component = get_random_value_from_normal_distribution(
            index=pop_visitors.index,
            mean=0.0,
            sd=np.sqrt(data_values.THERAPEUTIC_INERTIA_DYNAMIC_COMPONENT_VARIANCE),
            randomness=self.randomness,
            additional_key="sbp_therapeutic_inertia_dynamic_component",
        )
        sbp_prescription_inertia_propensity = stats.norm().cdf(
            pop_visitors[data_values.COLUMNS.SBP_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT]
            + sbp_therapeutic_inertia_dynamic_component
        )
        measured_sbp = self.get_measured_sbp(
            index=pop_visitors.index,
            exposure_pipeline=exposure_pipeline,
        )

        # Helper indexes and masks
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        ].index
        discontinued = pop_visitors[
            pop_visitors[data_values.COLUMNS.DISCONTINUED_SBP_MEDICATION]
        ].index
        low_sbp = measured_sbp[measured_sbp < data_values.SBP_THRESHOLD.LOW].index
        high_sbp = measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.HIGH].index
        medicated_high_sbp = currently_medicated.intersection(high_sbp)
        overcome_change_medication_inertia = pop_visitors[
            sbp_prescription_inertia_propensity
            > data_values.SBP_THERAPEUTIC_INERTIA.CHANGE_MEDICATION
        ].index
        overcome_first_medication_inertia = pop_visitors[
            sbp_prescription_inertia_propensity
            > data_values.SBP_THERAPEUTIC_INERTIA.FIRST_MEDICATION
        ].index
        # NOTE: we do not prescribe to people who have already discontinued medication
        newly_prescribed = (
            overcome_first_medication_inertia.difference(currently_medicated)
            .difference(discontinued)
            .difference(low_sbp)
        )
        mask_history_ihd_or_hf = (
            pop_visitors[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            != models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_SUSCEPTIBLE_STATE_NAME
        )
        mask_history_is = (
            pop_visitors[models.ISCHEMIC_STROKE_MODEL_NAME]
            != models.ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME
        )
        history_ihd_hf_or_is = pop_visitors[mask_history_ihd_or_hf | mask_history_is].index

        # [Treatment ramp ID C] Simulants who overcome therapeutic inertia, have
        # high SBP, and are not currently medicated (or discontinued)
        to_prescribe_c = newly_prescribed.intersection(high_sbp)
        # [Treatment ramp ID B] Simulants who overcome therapeutic inertia, have
        # medium-level SBP, and are not currently medicated (or discontinued)
        to_prescribe_b = newly_prescribed.difference(to_prescribe_c)
        # [Treatment ramp ID D] Simulants who overcome therapeutic inertia, have
        # high sbp, and are currently medicated
        to_prescribe_d = medicated_high_sbp.intersection(overcome_change_medication_inertia)

        # Prescribe medications for newly-medicated simulants (ramp IDs B and C)
        pop_visitors.loc[
            to_prescribe_b, data_values.COLUMNS.SBP_MEDICATION
        ] = self.randomness.choice(
            to_prescribe_b,
            choices=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["medium"].keys()
            ),
            p=list(
                data_values.FIRST_PRESCRIPTION_LEVEL_PROBABILITY["sbp"]["medium"].values()
            ),
            additional_key="medium_sbp_first_prescriptions",
        )
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

        # Change medications (ramp ID D)
        # Only move up if adherent and not already at the max ramp level
        adherent = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE]
            == data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
        ].index
        not_already_max_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
            != self.sbp_treatment_map[max(self.sbp_treatment_map)]
        ].index
        medication_change = to_prescribe_d.intersection(adherent).intersection(
            not_already_max_medicated
        )
        pop_visitors.loc[medication_change, data_values.COLUMNS.SBP_MEDICATION] = (
            pop_visitors[data_values.COLUMNS.SBP_MEDICATION].map(
                {v: k for k, v in self.sbp_treatment_map.items()}
            )
            + 1
        ).map(self.sbp_treatment_map)

        # Determine potential new outreach enrollees; applies to groups b, c, and
        # everyone already on medication
        # NOTE: simulants who discontinued medication are not candidates for enrollment
        if (self.scenario.is_outreach_scenario) or (self.scenario.is_polypill_scenario):
            maybe_enroll = to_prescribe_b.union(to_prescribe_c).union(currently_medicated)
        else:
            maybe_enroll = pd.Index([])  # baseline scenario

        # Determine potential new polypill enrollees; same as potential outreach
        # enrollees except must also have one of the following two requirements
        # 1. measured sbp >= 140
        # 2. measured sbp >= 130 and a history of MI or stroke
        # NOTE: simulants who discontinued medication are not candidates for enrollment
        if self.scenario.is_polypill_scenario:
            maybe_enroll = maybe_enroll.intersection(
                high_sbp.union(history_ihd_hf_or_is.difference(low_sbp))
            )

        return pop_visitors, maybe_enroll

    def apply_ldlc_treatment_ramp(
        self,
        pop_visitors: pd.DataFrame,
        ldlc_pipeline: Optional[Pipeline] = None,
        sbp_pipeline: Optional[Pipeline] = None,
    ) -> Tuple[pd.DataFrame, pd.Index]:
        """Applies the LDL-C treatment ramp.

        NOTE: simulants who have discontinued medication are not candidates
        for outreach or polypill interventions.
        NOTE: simulants who have discontinued medication are not candidates
        for being put (back on) medication.

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

        ldlc_therapeutic_inertia_dynamic_component = (
            get_random_value_from_normal_distribution(
                index=pop_visitors.index,
                mean=0.0,
                sd=np.sqrt(data_values.THERAPEUTIC_INERTIA_DYNAMIC_COMPONENT_VARIANCE),
                randomness=self.randomness,
                additional_key="ldlc_therapeutic_inertia_dynamic_component",
            )
        )
        ldlc_prescription_inertia_propensity = stats.norm().cdf(
            pop_visitors[data_values.COLUMNS.LDLC_THERAPEUTIC_INERTIA_CONSTANT_COMPONENT]
            + ldlc_therapeutic_inertia_dynamic_component
        )

        ascvd = self.get_ascvd(pop_visitors=pop_visitors, sbp_pipeline=sbp_pipeline)
        measured_ldlc = self.get_measured_ldlc(
            index=pop_visitors.index,
            exposure_pipeline=ldlc_pipeline,
        )

        # Helper indexes and masks
        currently_medicated = pop_visitors[
            pop_visitors[data_values.COLUMNS.LDLC_MEDICATION]
            != data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
        ].index
        discontinued = pop_visitors[
            pop_visitors[data_values.COLUMNS.DISCONTINUED_LDLC_MEDICATION]
        ].index
        overcome_prescription_inertia = pop_visitors[
            ldlc_prescription_inertia_propensity > data_values.LDLC_THERAPEUTIC_INERTIA
        ].index
        low_ascvd = ascvd[ascvd < data_values.ASCVD_THRESHOLD.LOW].index
        high_ascvd = ascvd[ascvd >= data_values.ASCVD_THRESHOLD.HIGH].index
        low_ldlc = measured_ldlc[measured_ldlc < data_values.LDLC_THRESHOLD.LOW].index
        above_medium_ldlc = measured_ldlc[
            measured_ldlc >= data_values.LDLC_THRESHOLD.MEDIUM
        ].index
        old_pop = pop_visitors[
            pop_visitors["age"] >= data_values.LDLC_OLD_AGE_THRESHOLD
        ].index
        # NOTE: we do not prescribe to people who have already discontinued medication
        newly_prescribed_young = (
            overcome_prescription_inertia.difference(currently_medicated)
            .difference(discontinued)
            .difference(low_ascvd)
            .difference(low_ldlc)
            .difference(old_pop)
        )
        newly_prescribed_old = (
            overcome_prescription_inertia.difference(currently_medicated)
            .difference(discontinued)
            .intersection(old_pop)
            .intersection(above_medium_ldlc)
        )
        treatment_change_eligible_young = (
            currently_medicated.difference(low_ascvd).difference(low_ldlc).difference(old_pop)
        )
        treatment_change_eligible_old = (
            currently_medicated.difference(low_ascvd)
            .intersection(above_medium_ldlc)
            .intersection(old_pop)
        )
        treatment_change_eligible = treatment_change_eligible_young.union(
            treatment_change_eligible_old
        )
        high_ldlc = measured_ldlc[measured_ldlc >= data_values.LDLC_THRESHOLD.HIGH].index
        mask_history_ihd_or_hf = (
            pop_visitors[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            != models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_SUSCEPTIBLE_STATE_NAME
        )
        mask_history_is = (
            pop_visitors[models.ISCHEMIC_STROKE_MODEL_NAME]
            != models.ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME
        )
        history_ihd_hf_or_is = pop_visitors[mask_history_ihd_or_hf | mask_history_is].index

        # [Treatment ramp ID D] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated (or discontinued), have
        # elevated ASCVD, and have a history of MI or IS
        to_prescribe_d = newly_prescribed_young.intersection(history_ihd_hf_or_is)
        # [Treatment ramp ID E] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated (or discontinued), have
        # elevated ASCVD, have no history of MI or IS, and who have high LDLC or ASCVD
        to_prescribe_e = newly_prescribed_young.difference(to_prescribe_d).intersection(
            high_ascvd.union(high_ldlc)
        )
        # [Treatment ramp ID F] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated (or discontinued), and are
        # either (1) old or (2) have elevated ASCVD, have no history of MI or IS,
        # but who do NOT have high LDLC or ASCVD
        to_prescribe_f_young = newly_prescribed_young.difference(to_prescribe_d).difference(
            to_prescribe_e
        )
        to_prescribe_f = to_prescribe_f_young.union(newly_prescribed_old)
        # [Treatment ramp ID G] Simulants who overcome therapeutic inertia, have
        # age-specific elevated LDLC, have elevated ASCVD, and are currently medicated
        to_prescribe_g = overcome_prescription_inertia.intersection(treatment_change_eligible)

        # Prescribe medications for newly-medicated simulants (ramp IDs D, E, and F)
        newly_prescribed = newly_prescribed_young.union(newly_prescribed_old)
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

        # Change medications (ramp ID G)
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

        # Determine potential new outreach enrollees; applies to groups
        # 'newly_prescribed' (d, e, f) and simulants already on medication
        # NOTE: simulants who discontinued medication are not candidates for enrollment
        if self.scenario.is_outreach_scenario:
            maybe_enroll = newly_prescribed.union(currently_medicated)
        else:
            maybe_enroll = pd.Index([])  # baseline or polypill scenario

        return pop_visitors, maybe_enroll

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
            # Update the outreach column with pipeline values
            pop_visitors.loc[to_enroll, data_values.COLUMNS.OUTREACH] = new_outreach.loc[
                to_enroll
            ]
            # Update the medication adherence columns with pipeline values
            pop_visitors.loc[
                to_enroll, data_values.COLUMNS.SBP_MEDICATION_ADHERENCE
            ] = self.sbp_medication_adherence(to_enroll)
            pop_visitors.loc[
                to_enroll, data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE
            ] = self.ldlc_medication_adherence(to_enroll)

            self.population_view.update(
                pop_visitors[
                    [
                        data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                        data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
                        data_values.COLUMNS.OUTREACH,
                    ]
                ]
            )

        return pop_visitors

    def enroll_in_polypill(
        self, pop_visitors: pd.DataFrame, maybe_enroll: pd.Index
    ) -> pd.DataFrame:
        """Enrolls simulants in polypill intervention program. It updates
        the polypill column as well as sbp medication level and adherence columns
        (the effects of polypill intervention).

        Reminder: 'cat1' polypill means enrolled and 'cat2' means not enrolled.
        """
        current_enrollment = pop_visitors.loc[maybe_enroll, data_values.COLUMNS.POLYPILL]
        updated_enrollment = self.polypill(maybe_enroll)
        to_enroll = current_enrollment[current_enrollment != updated_enrollment].index

        # Update the polypill statuses
        pop_visitors.loc[maybe_enroll, data_values.COLUMNS.POLYPILL] = updated_enrollment

        # Update sbp medication adherences
        if self.scenario.polypill_affects_sbp_adherence:
            # NOTE: this if statement is not strictly necessary because the
            # check is also made when registering the adherence value modifier
            # but is kept for readability
            pop_visitors.loc[
                to_enroll, data_values.COLUMNS.SBP_MEDICATION_ADHERENCE
            ] = self.sbp_medication_adherence(to_enroll)

        # Update sbp medication levels
        if self.scenario.polypill_affects_sbp_medication:
            low_sbp_medication_dose = pop_visitors[
                pop_visitors[data_values.COLUMNS.SBP_MEDICATION].map(
                    {v: k for k, v in self.sbp_treatment_map.items()}
                )
                < data_values.SBP_MEDICATION_LEVEL.THREE_DRUGS_HALF_DOSE.VALUE
            ].index
            pop_visitors.loc[
                to_enroll.intersection(low_sbp_medication_dose),
                data_values.COLUMNS.SBP_MEDICATION,
            ] = data_values.SBP_MEDICATION_LEVEL.THREE_DRUGS_HALF_DOSE.DESCRIPTION

        self.population_view.update(
            pop_visitors[
                [
                    data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
                    data_values.COLUMNS.POLYPILL,
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
