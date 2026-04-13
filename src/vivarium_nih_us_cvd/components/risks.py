from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import SimulantData
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


class DropValueRisk(Risk):
    """Risk which has a "drop value" applied to it in post-processing.
    Note: This post-processor will overwrite the post-processor for an exposure with
    category thresholds defined in the config."""

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self, risk: str):
        super().__init__(risk)
        self.raw_exposure_pipeline_name = f"{self.risk.name}.raw_exposure"
        self.drop_value_pipeline_name = f"{self.risk.name}.drop_value"

    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        self.raw_exposure = builder.value.register_value_producer(
            self.raw_exposure_pipeline_name,
            source=self.get_current_exposure,
        )
        self.drop_value = builder.value.register_value_producer(
            self.drop_value_pipeline_name,
            source=lambda index: pd.Series(0.0, index=index),
        )

    #################
    # Setup methods #
    #################

    def register_exposure_pipeline(self, builder: Builder) -> None:
        """Override to apply drop-value post-processing to the exposure pipeline."""
        drop_value_pipeline = builder.value.get_value(self.drop_value_pipeline_name)

        def drop_value_post_processor(index, exposure, _manager):
            drop_values = drop_value_pipeline(index)
            return exposure - drop_values

        builder.value.register_attribute_producer(
            self.exposure_name,
            source=self.get_current_exposure,
            preferred_post_processor=drop_value_post_processor,
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def get_current_exposure(self, index: pd.Index) -> pd.Series:
        return self.exposure_ppf(index)


class CorrelatedRisk(DropValueRisk):
    """Creates risk without propensities, so they can be created by correlation component"""

    ##############
    # Properties #
    ##############

    @property
    def columns_created(self) -> List[str]:
        return []

    @property
    def columns_required(self) -> Optional[List[str]]:
        return [self.propensity_name]

    @property
    def initialization_requirements(self) -> Dict[str, List[str]]:
        return {
            "requires_columns": [],
            "requires_values": [],
            "requires_streams": [],
        }

    #####################
    # Lifecycle methods #
    #####################

    def setup(self, builder: Builder) -> None:
        # Do most of Risk.setup() but skip propensity initializer registration.
        # RiskCorrelation creates the propensity columns instead.
        self._components = builder.components
        self.distribution_type = self.get_distribution_type(builder)
        self.exposure_distribution = self.get_exposure_distribution(builder)
        self.randomness = self.get_randomness_stream(builder)

        # The vph 5 RiskExposureDistribution registers an attribute
        # pipeline ``<risk>.exposure_distribution.ppf`` that, given an
        # index, returns inverse-CDF exposures from the propensity
        # column. We use this in our overridden ``get_current_exposure``
        # below in place of the old ``distribution.ppf(propensity)``
        # call (which is no longer part of the vph 5 distribution API).
        self.exposure_ppf = builder.value.get_value(
            self.exposure_distribution.exposure_ppf_pipeline
        )

        # Register drop_value and raw_exposure pipelines
        self.drop_value = builder.value.register_value_producer(
            self.drop_value_pipeline_name,
            source=lambda index: pd.Series(0.0, index=index),
        )
        self.raw_exposure = builder.value.register_value_producer(
            self.raw_exposure_pipeline_name,
            source=self.get_current_exposure,
        )

        # Register the main exposure pipeline (with drop-value post-processing)
        self.register_exposure_pipeline(builder)

        # Check for non-loglinear risk effects. The standard vph 5 Risk
        # checks for components named "non_log_linear_risk_effect.{risk}_on_*"
        # but our NonLogLinearPAFCalculationRiskEffect and
        # NonLogLinearMediatedRiskEffect use different name prefixes.
        # Check by isinstance instead to catch all variants.
        from vivarium_public_health.risks.effect import NonLogLinearRiskEffect as _NLLRE

        self.includes_non_loglinear_risk_effect = False
        for c in builder.components.list_components():
            if f".{self.risk.name}" not in c:
                continue
            try:
                if isinstance(builder.components.get_component(c), _NLLRE):
                    self.includes_non_loglinear_risk_effect = True
                    break
            except Exception:
                continue
        if self.includes_non_loglinear_risk_effect:
            builder.population.register_initializer(
                initializer=self.initialize_exposure,
                columns=self.exposure_column_name,
                required_resources=[self.exposure_name, self.propensity_name],
            )

    def get_exposure(self, index: pd.Index) -> pd.Series:
        """Cache the exposure value for non-loglinear risk effects.

        Override the parent to skip post-processors during population
        initialization, since Treatment modifiers may reference columns
        (e.g. ``lifestyle``) that haven't been initialized yet.
        The non-loglinear RR lookup only needs the *final* exposure;
        ``on_time_step_prepare`` updates the cache each step with the
        fully-post-processed value from the normal pipeline.
        """
        exposure = self.population_view.get(
            index, self.exposure_name, skip_post_processor=True
        )
        exposure.name = self.exposure_column_name
        return exposure

    ########################
    # Event-driven methods #
    ########################

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        pass


class AdjustedRisk(CorrelatedRisk):
    """Manages raw gbd exposure and adjusted/untreated exposure pipelines"""

    ##############
    # Properties #
    ##############

    @property
    def columns_required(self) -> Optional[List[str]]:
        columns_required = super().columns_required
        if self.multiplier_col:
            columns_required.append(self.multiplier_col)
        return columns_required

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self, risk: str):
        super().__init__(risk)
        self.gbd_exposure_pipeline_name = f"{self.risk.name}.gbd_exposure"
        self.multiplier_col = {
            "risk_factor.high_systolic_blood_pressure": COLUMNS.SBP_MULTIPLIER,
            "risk_factor.high_ldl_cholesterol": COLUMNS.LDLC_MULTIPLIER,
        }.get(self.risk, None)

    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        self.gbd_exposure = builder.value.register_value_producer(
            self.gbd_exposure_pipeline_name,
            source=self.get_gbd_exposure,
            preferred_post_processor=get_exposure_post_processor(builder, self.risk),
        )

    #################
    # Setup methods #
    #################

    def register_exposure_pipeline(self, builder: Builder) -> None:
        """Override to use medication multiplier + drop value."""
        drop_value_pipeline = builder.value.get_value(self.drop_value_pipeline_name)

        def drop_value_post_processor(index, exposure, _manager):
            drop_values = drop_value_pipeline(index)
            return exposure - drop_values

        builder.value.register_attribute_producer(
            self.exposure_name,
            source=self.get_current_exposure,
            preferred_post_processor=drop_value_post_processor,
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def get_gbd_exposure(self, index: pd.Index) -> pd.Series:
        """Gets the raw gbd exposures and applies upper/lower limits"""
        exposures = self.exposure_ppf(index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("minimum", None)
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("maximum", None)
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures

    def get_current_exposure(self, index: pd.Index) -> pd.Series:
        """Applies medication multipliers to the raw GBD exposure values"""
        if self.multiplier_col:
            return (
                self.gbd_exposure(index)
                * self.population_view.get(index, [self.multiplier_col])[self.multiplier_col]
            )
        else:
            return self.gbd_exposure(index)


class TruncatedRisk(CorrelatedRisk):
    """Keep exposure values between defined limits"""

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def get_current_exposure(self, index: pd.Index) -> pd.Series:
        # Keep exposure values between defined limits
        exposures = self.exposure_ppf(index)
        min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("minimum", None)
        max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name].get("maximum", None)
        exposures[exposures < min_exposure] = min_exposure
        exposures[exposures > max_exposure] = max_exposure

        return exposures


class CategoricalSBPRisk(Component):
    """Bin continuous systolic blood pressure values into categories"""

    CONFIGURATION_DEFAULTS = {
        "risk": {
            "exposure": "data",
            "rebinned_exposed": [],
            "category_thresholds": [],
        }
    }

    ##############
    # Properties #
    ##############

    @property
    def configuration_defaults(self) -> Dict[str, Any]:
        return {self.risk.name: self.CONFIGURATION_DEFAULTS["risk"]}

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self):
        super().__init__()
        self.risk = EntityString("risk_factor.categorical_high_systolic_blood_pressure")
        self.exposure_pipeline_name = f"{self.risk.name}.exposure"

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        # SBP exposure is registered as an attribute pipeline by
        # AdjustedRisk; the patched ``ValuesManager.get_value`` (in
        # ``vivarium_nih_us_cvd.plugins``) routes the lookup to the
        # attribute pipeline so this still returns a callable that
        # accepts an index and yields the per-simulant SBP value.
        self.continuous_exposure = builder.value.get_value(PIPELINES.SBP_EXPOSURE)
        # Register as attribute pipeline so that vph 5's RiskEffect /
        # MediatedRiskEffect can declare it as a required_resource and
        # read it via population_view.get().  The monkey-patch in
        # plugins/__init__.py ensures existing get_value() callers
        # still find it.
        self.exposure = builder.value.register_attribute_producer(
            self.exposure_pipeline_name,
            source=self.get_current_exposure,
            required_resources=[PIPELINES.SBP_EXPOSURE],
        )

    ##################################
    # Pipeline sources and modifiers #
    ##################################

    def get_current_exposure(self, index: pd.Index) -> pd.Series:
        continuous_exposure = self.continuous_exposure(index)

        bins = [
            0,
            CATEGORICAL_SBP_INTERVALS.CAT3_LEFT_THRESHOLD,
            CATEGORICAL_SBP_INTERVALS.CAT2_LEFT_THRESHOLD,
            CATEGORICAL_SBP_INTERVALS.CAT1_LEFT_THRESHOLD,
            np.inf,
        ]

        categorical_exposure = pd.cut(
            continuous_exposure,
            bins=bins,
            labels=["cat4", "cat3", "cat2", "cat1"],
            right=False,
        )  # left interval is closed, right interval is open

        return categorical_exposure
