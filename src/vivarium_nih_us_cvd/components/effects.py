from typing import Callable

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.time import get_time_stamp

from vivarium_nih_us_cvd.constants import data_values


class OutreachEffect:
    """A component to model the impact of the outreach risk on medication adherence levels"""

    def __init__(self):
        pass

    def __repr__(self):
        return "OutreachEffect"

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return f"outreach_effect"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.simulation_start_time = get_time_stamp(builder.configuration.time.start)
        self.randomness = builder.randomness.get_stream(self.name)
        self.exposure = builder.value.get_value(data_values.PIPELINES.OUTREACH_EXPOSURE)
        self._register_target_modifiers(builder)

    def _register_target_modifiers(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE,
            modifier=self._sbp_adherence_modifier(builder),
        )

        builder.value.register_value_modifier(
            data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE,
            modifier=self._ldlc_adherence_modifier(builder),
        )

    def _sbp_adherence_modifier(self, builder: Builder) -> Callable[[pd.Index, pd.Series], pd.Series]:
        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            return self._adjust_target(index=index, target=target, medication_type="sbp")
        
        return adjust_target

    def _ldlc_adherence_modifier(self, builder: Builder) -> Callable[[pd.Index, pd.Series], pd.Series]:
        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            return self._adjust_target(index=index, target=target, medication_type="ldlc")
        
        return adjust_target

    def _adjust_target(self, index: pd.Index, target: pd.Series, medication_type: str) -> pd.Series:
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
                additional_key=f"adjust_{medication_type}_medication_adherence_values",
            )
            target[primary_non_adherent] = adjusted

        return target
