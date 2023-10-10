from typing import Callable, Optional

import numpy as np
import pandas as pd
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium.framework.lookup import LookupTable
from vivarium.framework.time import get_time_stamp
from vivarium_public_health.risks.effect import RiskEffect

from vivarium_nih_us_cvd.constants import data_keys, data_values, scenarios
from vivarium_nih_us_cvd.constants.scenarios import InterventionScenario


class InterventionAdherenceEffect(Component):
    """A component to model the impact of the intervention risks on medication adherence levels"""

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.polypill = builder.value.get_value(data_values.PIPELINES.POLYPILL_EXPOSURE)
        self.scenario = self._get_scenario(builder)
        self.clock = builder.time.clock()
        self.simulation_start_time = get_time_stamp(builder.configuration.time.start)
        self.randomness = builder.randomness.get_stream(self.name)
        self._register_target_modifiers(builder)

    def _get_scenario(self, builder: Builder) -> InterventionScenario:
        return scenarios.INTERVENTION_SCENARIOS[builder.configuration.intervention.scenario]

    def _register_target_modifiers(self, builder: Builder) -> None:
        """which target modifiers get registered depends on the scenario"""
        if self.scenario.is_outreach_scenario:
            builder.value.register_value_modifier(
                data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE,
                modifier=self._outreach_sbp_adherence_modifier,
            )
            builder.value.register_value_modifier(
                data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE,
                modifier=self._outreach_ldlc_adherence_modifier,
            )
        if self.scenario.polypill_affects_sbp_adherence:
            builder.value.register_value_modifier(
                "risk_factor.sbp_medication_adherence.exposure_parameters",
                modifier=self._polypill_sbp_adherence_modifier,
                requires_values=[data_values.PIPELINES.POLYPILL_EXPOSURE],
            )

    def _outreach_sbp_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        return self._outreach_adjust_target(index=index, target=target, medication_type="sbp")

    def _outreach_ldlc_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        return self._outreach_adjust_target(
            index=index, target=target, medication_type="ldlc"
        )

    def _outreach_adjust_target(
        self, index: pd.Index, target: pd.Series, medication_type: str
    ) -> pd.Series:
        clock_time = self.clock()
        # Do not adjust target on intialization. We do this because during
        # initialization the Treatment component sets the medication adherence
        # state table columns equal to the medication adherence pipeline values;
        # this modifier implements the outreach intervention treatment effect
        # which we do want to be active upon initialization
        if clock_time >= self.simulation_start_time:  # not initialization
            primary_non_adherent = target[
                target == data_values.MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT
            ].index
            target[primary_non_adherent] = self.randomness.choice(
                primary_non_adherent,
                choices=list(data_values.OUTREACH_EFFECTS[medication_type].keys()),
                p=list(data_values.OUTREACH_EFFECTS[medication_type].values()),
                additional_key=f"outreach_adjust_{medication_type}_medication_adherence_values",
            )

        return target

    def _polypill_sbp_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        polypill = self.polypill(index)
        on_polypill = polypill[polypill == "cat1"].index
        for (
            cat,
            probability,
        ) in data_values.POLYPILL_SBP_MEDICATION_ADHERENCE_COVERAGE.items():
            target.loc[on_polypill, cat] = probability

        return target


MEDIATOR_NAMES = {
    "high_body_mass_index_in_adults": {
        "acute_ischemic_stroke": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "chronic_ischemic_stroke_to_acute_ischemic_stroke": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "acute_myocardial_infarction": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "post_myocardial_infarction_to_acute_myocardial_infarction": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "heart_failure_from_ischemic_heart_disease": [
            "categorical_high_systolic_blood_pressure",
        ],
        "heart_failure_residual": [
            "categorical_high_systolic_blood_pressure",
        ],
    },
    "high_fasting_plasma_glucose": {
        "acute_ischemic_stroke": [
            "high_ldl_cholesterol",
        ],
        "chronic_ischemic_stroke_to_acute_ischemic_stroke": [
            "high_ldl_cholesterol",
        ],
        "acute_myocardial_infarction": [
            "high_ldl_cholesterol",
        ],
        "post_myocardial_infarction_to_acute_myocardial_infarction": [
            "high_ldl_cholesterol",
        ],
    },
}


class MediatedRiskEffect(RiskEffect):
    """Applies mediation to risk effects"""

    def setup(self, builder):
        super().setup(builder)
        # Register unadjusted RR pipelines by passing target=1s to the super's target_modifier
        self.is_target_hf = self.target.name.startswith("heart_failure")
        self.unadjusted_rr = builder.value.register_value_producer(
            f"unadjusted_rr_{self.risk.name}_on_{self.target.name}",
            source=lambda idx: self.target_modifier(idx, pd.Series(1.0, index=idx)),
        )
        self.mediators = MEDIATOR_NAMES.get(self.risk.name, {}).get(self.target.name, [])
        self.unadjusted_mediator_rr = {
            mediator: builder.value.get_value(
                f"unadjusted_rr_{mediator}_on_{self.target.name}"
            )
            for mediator in self.mediators
        }
        # Register the mediation target modifier
        self.mediated_target_modifier = self.get_mediated_target_modifier(builder)
        self.register_mediated_target_modifier(builder)

    def register_target_modifier(self, builder: Builder) -> None:
        """We do not want to register the super's target modifier, so we override it here"""
        pass

    def get_mediated_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        if self.is_target_hf:
            delta_data = builder.data.load(data_keys.MEDIATION.HF_DELTAS)
            deltas = builder.lookup.build_table(
                delta_data, parameter_columns=["age"], key_columns=["sex"]
            )

            def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
                unadjusted_rr = self.unadjusted_rr(index)
                scaling_factor = pd.Series(1.0, index=index)
                for mediator in self.mediators:
                    unadjusted_mediator_rr = self.unadjusted_mediator_rr[mediator](index)
                    # NOTE: We only adjust the target RR if the mediator RR is not 1 (TMREL).
                    #   Though not strictly required, it will save computation time.
                    #
                    #   We also only adjust the target RR if the risk RR is not 1 (TMREL)
                    #   in order to be consistent with mediation for IHD and stroke.
                    not_tmrel_idx = index[
                        (unadjusted_mediator_rr != 1.0) & (unadjusted_rr != 1.0)
                    ]
                    scaling_factor.loc[not_tmrel_idx] *= unadjusted_mediator_rr.loc[
                        not_tmrel_idx
                    ] ** deltas(not_tmrel_idx)
                return target * unadjusted_rr / scaling_factor

        else:
            mediation_factors = builder.data.load(data_keys.MEDIATION.MEDIATION_FACTORS)
            mediation_factors = mediation_factors.loc[
                (mediation_factors["risk_name"] == self.risk.name)
                & (mediation_factors["affected_entity"] == self.target.name)
            ]

            def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
                unadjusted_rr = self.unadjusted_rr(index)
                scaling_factor = pd.Series(1.0, index=index)
                for mediator in self.mediators:
                    unadjusted_mediator_rr = self.unadjusted_mediator_rr[mediator](index)
                    # NOTE: We only adjust the target RR if the mediator RR is not 1 (TMREL)
                    #   to prevent divide-by-0 errors (since log(1) = 0); it also makes sense
                    #   since RR**x = 1 when RR = 1.
                    #
                    #   We also only adjust the target RR if the risk RR is not 1 (TMREL).
                    #   This is not necessarily required since that would result in delta = 0
                    #   and a scaling factor would resolve to 1 always, but it will
                    #   save some computation time.
                    not_tmrel_idx = index[
                        (unadjusted_mediator_rr != 1.0) & (unadjusted_rr != 1.0)
                    ]
                    mf = mediation_factors.loc[
                        mediation_factors["mediator_name"] == mediator, "value"
                    ].values[0]
                    delta_mediator = np.log(
                        mf * (unadjusted_rr.loc[not_tmrel_idx] - 1) + 1
                    ) / np.log(unadjusted_mediator_rr.loc[not_tmrel_idx])
                    scaling_factor.loc[not_tmrel_idx] *= (
                        unadjusted_mediator_rr.loc[not_tmrel_idx] ** delta_mediator
                    )
                return target * unadjusted_rr / scaling_factor

        return adjust_target

    def register_mediated_target_modifier(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            self.target_pipeline_name,
            modifier=self.mediated_target_modifier,
            requires_values=[f"{self.risk.name}.exposure"],
            requires_columns=["age", "sex"],
        )


class PAFCalculationRiskEffect(MediatedRiskEffect):
    """Risk effect component for calculating PAFs"""

    def get_population_attributable_fraction_source(
        self, builder: Builder
    ) -> Optional[LookupTable]:
        return None

    def register_target_modifier(self, builder: Builder) -> None:
        pass

    def register_paf_modifier(self, builder: Builder) -> None:
        pass
