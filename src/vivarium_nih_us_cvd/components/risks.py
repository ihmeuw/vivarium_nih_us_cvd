import pandas as pd
import numpy as np
from typing import Dict
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import PopulationView
from vivarium.framework.values import Pipeline
from vivarium_public_health.risks.base_risk import Risk
from vivarium_public_health.risks.data_transformations import (
    get_exposure_post_processor,
)
from vivarium_public_health.utilities import EntityString

from vivarium_nih_us_cvd.constants.data_values import (
    CATEGORICAL_SBP_INTERVALS,
    COLUMNS,
    PIPELINES,
    RISK_EXPOSURE_LIMITS,
)


class AdjustedRisk(Risk):
    """Manages raw gbd exposure and adjusted/untreated exposure pipelines"""

    def __init__(self, risk: str):
        super().__init__(risk)
        self.gbd_exposure_pipeline_name = f"{self.risk.name}.gbd_exposure"
        self.multiplier_col = {
            "risk_factor.high_systolic_blood_pressure": COLUMNS.SBP_MULTIPLIER,
            "risk_factor.high_ldl_cholesterol": COLUMNS.LDLC_MULTIPLIER,
        }.get(self.risk, None)

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
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("minimum", None)
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("maximum", None)
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        """Applies medication multipliers to the raw GBD exposure values"""
        if self.multiplier_col:
            return (
                self.gbd_exposure(index)
                * self.population_view.get(index)[self.multiplier_col]
            )
        else:
            return self.gbd_exposure(index)


class TruncatedRisk(Risk):
    """Keep exposure values between defined limits"""

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        # Keep exposure values between defined limits
        propensity = self.propensity(index)

        exposures = pd.Series(self.exposure_distribution.ppf(propensity), index=index)
        min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("minimum", None)
        max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("maximum", None)
        exposures[exposures < min_exposure] = min_exposure
        exposures[exposures > max_exposure] = max_exposure

        return exposures


class CategoricalSBPRisk:
    """Bin continuous systolic blood pressure values into categories"""

    configuration_defaults = {
        "risk": {
            "exposure": "data",
            "rebinned_exposed": [],
            "category_thresholds": [],
        }
    }

    def __init__(self):
        self.risk = EntityString("risk_factor.categorical_high_systolic_blood_pressure")
        self.configuration_defaults = self._get_configuration_defaults()
        self.exposure_pipeline_name = f"{self.risk.name}.exposure"

    def __repr__(self) -> str:
        return "CategoricalSBPRisk()"

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {self.risk.name: Risk.configuration_defaults["risk"]}

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return f"risk.{self.risk}"

    #################
    # Setup methods #
    #################

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        self.continuous_exposure = builder.value.get_value(PIPELINES.SBP_EXPOSURE)
        self.exposure = self._get_exposure_pipeline(builder)

    def _get_exposure_pipeline(self, builder: Builder) -> Pipeline:
        return builder.value.register_value_producer(
            self.exposure_pipeline_name,
            source=self._get_current_exposure,
            requires_values=[PIPELINES.SBP_EXPOSURE],
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        continuous_exposure = self.continuous_exposure(index)

        bins = [
            0,
            CATEGORICAL_SBP_INTERVALS.CAT3_LEFT_THRESHOLD,
            CATEGORICAL_SBP_INTERVALS.CAT2_LEFT_THRESHOLD,
            CATEGORICAL_SBP_INTERVALS.CAT1_LEFT_THRESHOLD,
            #RISK_EXPOSURE_LIMITS["high_systolic_blood_pressure"]["maximum"],
            np.inf,
        ]

        categorical_exposure = pd.cut(
            continuous_exposure,
            bins=bins,
            labels=["cat4", "cat3", "cat2", "cat1"],
            right=False,
        )  # left interval is closed, right interval is open

        return categorical_exposure
