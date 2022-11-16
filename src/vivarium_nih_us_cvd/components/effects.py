from typing import Callable

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.time import get_time_stamp

from vivarium_nih_us_cvd.constants import data_values, scenarios


class InterventionEffect:
    """A component to model the impact of the intervention risks on medication adherence levels"""

    def __repr__(self):
        return "InterventionEffect"

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return "intervention_effect"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.scenario = self._get_scenario(builder)
        self.clock = builder.time.clock()
        self.simulation_start_time = get_time_stamp(builder.configuration.time.start)
        self.randomness = builder.randomness.get_stream(self.name)
        self._register_target_modifiers(builder)

    def _get_scenario(self, builder: Builder) -> bool:
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
        elif self.scenario.is_polypill_scenario:
            builder.value.register_value_modifier(
                "risk_factor.sbp_medication_adherence.exposure_parameters",
                modifier=self._polypill_sbp_adherence_modifier,
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
            adjusted = self.randomness.choice(
                primary_non_adherent,
                choices=list(data_values.OUTREACH_EFFECTS[medication_type].keys()),
                p=list(data_values.OUTREACH_EFFECTS[medication_type].values()),
                additional_key=f"outreach_adjust_{medication_type}_medication_adherence_values",
            )
            target[primary_non_adherent] = adjusted

        return target

    def _polypill_sbp_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        return self._polypill_adjust_target(index=index, target=target)

    def _polypill_adjust_target(self, index: pd.Index, target: pd.Series) -> pd.Series:
        clock_time = self.clock()
        # Do not adjust target on intialization. We do this because during
        # initialization the Treatment component sets the medication adherence
        # state table columns equal to the medication adherence pipeline values;
        # this modifier implements the outreach intervention treatment effect
        # which we do want to be active upon initialization
        if clock_time >= self.simulation_start_time:  # not initialization
            adjusted = target.copy()
            for (
                cat,
                probability,
            ) in data_values.POLYPILL_SBP_MEDICATION_ADHERENCE_COVERAGE.items():
                adjusted[cat] = probability
            target = adjusted

        return target
