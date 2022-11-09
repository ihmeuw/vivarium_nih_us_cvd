from typing import Callable

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.time import get_time_stamp

from vivarium_nih_us_cvd.constants import data_values


class OutreachEffect:
    """A component to model the impact of the outreach risk on medication adherence levels"""

    def __init__(self):
        self.exposure_pipeline_name = data_values.PIPELINES.OUTREACH_EXPOSURE
        self.sbp_target_pipeline_name = (
            data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE
        )
        self.ldlc_target_pipeline_name = (
            data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE
        )

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
        self.randomness = builder.randomness.get_stream(self.name)
        self.population_view = builder.population.get_view(["age", "sex"])
        self.exposure = builder.value.get_value(self.exposure_pipeline_name)
        self.sbp_medication_adherence_target_modifier = (
            self._get_sbp_medication_adherence_target_modifier(builder)
        )
        self.ldlc_medication_adherence_target_modifier = (
            self._get_ldlc_medication_adherence_target_modifier(builder)
        )
        self._register_target_modifiers(builder)

    def _get_sbp_medication_adherence_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            return self._adjust_target(
                index=index, target=target, medication_type="sbp", builder=builder
            )

        return adjust_target

    def _get_ldlc_medication_adherence_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            return self._adjust_target(
                index=index, target=target, medication_type="ldlc", builder=builder
            )

        return adjust_target

    def _adjust_target(
        self, index: pd.Index, target: pd.Series, medication_type: str, builder: Builder
    ) -> pd.Series:
        clock_time = builder.time.clock()()
        simulation_start_time = get_time_stamp(builder.configuration.time.start)
        # Do not adjust target on intialization
        if clock_time >= simulation_start_time:  # not initialization
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

    def _register_target_modifiers(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            self.sbp_target_pipeline_name,
            modifier=self.sbp_medication_adherence_target_modifier,
        )

        builder.value.register_value_modifier(
            self.ldlc_target_pipeline_name,
            modifier=self.ldlc_medication_adherence_target_modifier,
        )
