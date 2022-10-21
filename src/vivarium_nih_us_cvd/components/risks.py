from typing import Callable

import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import PopulationView
from vivarium.framework.values import Pipeline
from vivarium_public_health.risks.base_risk import Risk as Risk_
from vivarium_public_health.risks.data_transformations import (
    get_exposure_post_processor,
)

from vivarium_nih_us_cvd.constants.data_values import (
    COLUMNS,
    MEDICATION_ADHERENCE_SCORE,
    RISK_EXPOSURE_LIMITS,
    SBP_MEDICATION_LEVEL,
)
from vivarium_nih_us_cvd.constants.paths import FILEPATHS

# Format the SBP risk effects file and generate bin edges
sbp_risk_effects = pd.read_csv(FILEPATHS.SBP_MEDICATION_EFFECTS)
sbp_risk_effects = sbp_risk_effects.melt(
    id_vars=["sbp_start_exclusive", "sbp_end_inclusive"],
    value_vars=[
        c.DESCRIPTION
        for c in SBP_MEDICATION_LEVEL
        if c.DESCRIPTION != SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
    ],
    var_name=COLUMNS.SBP_MEDICATION,
)
sbp_risk_effects.loc[
    (sbp_risk_effects["sbp_start_exclusive"].isna()), "sbp_start_exclusive"
] = -np.inf
sbp_risk_effects.loc[
    (sbp_risk_effects["sbp_end_inclusive"].isna()), "sbp_end_inclusive"
] = np.inf
sbp_bin_edges = sorted([x for x in sbp_risk_effects["sbp_end_inclusive"].unique()])
# Need to add on -inf
sbp_bin_edges = [-np.inf] + sbp_bin_edges


class Risk(Risk_):
    """Use the standard vivarium_public_health Risk class for risk
    exposure except apply limits to lower and upper bounds (when defined).
    """

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        exposures = super()._get_current_exposure(index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["minimum"]
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["maximum"]
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures


class SBPRisk(Risk_):
    """Manages gbd SBP exposure and untreated SBP exposure pipelines"""

    def __init__(self, risk: str):
        super().__init__(risk)
        self.gbd_exposure_pipeline_name = f"{self.risk.name}.gbd_exposure"

    def __repr__(self) -> str:
        return f"Risk({self.risk})"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        self.gbd_exposure = self._get_gbd_exposure_pipeline(builder)
        self.target_modifier = self._get_target_modifier(builder)
        self._register_target_modifier(builder)

    def _get_gbd_exposure_pipeline(self, builder: Builder) -> Pipeline:
        return builder.value.register_value_producer(
            self.gbd_exposure_pipeline_name,
            source=self._get_gbd_exposure,
            requires_columns=["age", "sex"],
            requires_values=[self.propensity_pipeline_name],
            preferred_post_processor=get_exposure_post_processor(builder, self.risk),
        )

    def _get_exposure_pipeline(self, builder: Builder) -> Pipeline:
        return builder.value.register_value_producer(
            self.exposure_pipeline_name,
            source=self._get_current_exposure,
            requires_columns=[COLUMNS.SBP_MULTIPLIER],
            requires_values=[self.gbd_exposure_pipeline_name],
            preferred_post_processor=get_exposure_post_processor(builder, self.risk),
        )

    def _get_population_view(self, builder: Builder) -> PopulationView:
        return builder.population.get_view(
            [
                self.propensity_column_name,
                COLUMNS.SBP_MULTIPLIER,
                COLUMNS.SBP_MEDICATION,
                COLUMNS.SBP_MEDICATION_ADHERENCE,
            ]
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def _get_gbd_exposure(self, index: pd.Index) -> pd.Series:
        """Gets the raw gbd exposures and applies upper/lower limits"""
        # TODO: Confirm that this is correct to apply the limits to the gbd
        # exposure rather than the untreated exposure values
        propensity = self.propensity(index)
        exposures = pd.Series(self.exposure_distribution.ppf(propensity), index=index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["minimum"]
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["maximum"]
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        """Applies medication multipliers to the raw GBD exposure values"""

        return (
            self.gbd_exposure(index) * self.population_view.get(index)[COLUMNS.SBP_MULTIPLIER]
        )

    def _get_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        """Apply medication effects"""

        def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
            """Determine the exposure decrease as treatment_efficacy * adherence_score"""
            pop_view = self.population_view.get(index)
            adherence_scores = pop_view[COLUMNS.SBP_MEDICATION_ADHERENCE].map(
                MEDICATION_ADHERENCE_SCORE
            )
            df_efficacy = pd.DataFrame(
                {"bin": pd.cut(x=target, bins=sbp_bin_edges, right=True)}
            )
            df_efficacy["sbp_start_exclusive"] = df_efficacy["bin"].apply(lambda x: x.left)
            df_efficacy["sbp_end_inclusive"] = df_efficacy["bin"].apply(lambda x: x.right)
            df_efficacy = pd.concat([df_efficacy, pop_view[COLUMNS.SBP_MEDICATION]], axis=1)
            df_efficacy = (
                df_efficacy.reset_index()
                .merge(
                    sbp_risk_effects,
                    on=["sbp_start_exclusive", "sbp_end_inclusive", COLUMNS.SBP_MEDICATION],
                    how="left",
                )
                .set_index("index")
            )
            # Simulants not on treatment mean 0 effect
            assert set(
                df_efficacy.loc[df_efficacy["value"].isna(), COLUMNS.SBP_MEDICATION]
            ) == {SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION}
            df_efficacy.loc[
                df_efficacy[COLUMNS.SBP_MEDICATION]
                == SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION,
                "value",
            ] = 0
            assert df_efficacy["value"].isna().sum() == 0
            treatment_efficacy = df_efficacy["value"]

            sbp_decrease = treatment_efficacy * adherence_scores

            return target - sbp_decrease

        return adjust_target

    def _register_target_modifier(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            self.exposure_pipeline_name,
            modifier=self.target_modifier,
            # requires_values=[f"{self.risk.name}.exposure"],
            requires_columns=[COLUMNS.SBP_MEDICATION, COLUMNS.SBP_MEDICATION_ADHERENCE],
        )
