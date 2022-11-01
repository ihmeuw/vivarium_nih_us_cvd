import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import PopulationView
from vivarium.framework.values import Pipeline
from vivarium_public_health.risks.base_risk import Risk
from vivarium_public_health.risks.data_transformations import (
    get_exposure_post_processor,
)

from vivarium_nih_us_cvd.constants.data_values import COLUMNS, RISK_EXPOSURE_LIMITS


class AdjustedRisk(Risk):
    """Manages raw gbd exposure and adjusted/untreated exposure pipelines"""

    def __init__(self, risk: str):
        super().__init__(risk)
        self.gbd_exposure_pipeline_name = f"{self.risk.name}.gbd_exposure"
        self.multiplier_col = {
            "risk_factor.high_systolic_blood_pressure": COLUMNS.SBP_MULTIPLIER,
            "risk_factor.high_ldl_cholesterol": COLUMNS.LDLC_MULTIPLIER,
        }[self.risk]

    def __repr__(self) -> str:
        return f"AdjustedRisk({self.risk})"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        self.gbd_exposure = self._get_gbd_exposure_pipeline(builder)

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
            requires_columns=[self.multiplier_col],
            requires_values=[self.gbd_exposure_pipeline_name],
            preferred_post_processor=get_exposure_post_processor(builder, self.risk),
        )

    def _get_population_view(self, builder: Builder) -> PopulationView:
        return builder.population.get_view(
            [
                self.propensity_column_name,
                self.multiplier_col,
            ]
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def _get_gbd_exposure(self, index: pd.Index) -> pd.Series:
        """Gets the raw gbd exposures and applies upper/lower limits"""
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
        return self.gbd_exposure(index) * self.population_view.get(index)[self.multiplier_col]
