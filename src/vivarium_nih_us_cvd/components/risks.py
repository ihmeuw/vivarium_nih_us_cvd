from typing import Dict, List

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import PopulationView, SimulantData
from vivarium.framework.randomness import RandomnessStream
from vivarium.framework.values import Pipeline
from vivarium_public_health.risks.base_risk import Risk as Risk_
from vivarium_public_health.risks.data_transformations import (
    get_exposure_post_processor,
)
from vivarium_public_health.risks.distributions import SimulationDistribution
from vivarium_public_health.utilities import EntityString

from vivarium_nih_us_cvd.constants.data_values import COLUMNS, RISK_EXPOSURE_LIMITS


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


class SBPRisk:
    """Manages gbd SBP exposure and untreated SBP exposure pipelines"""

    configuration_defaults = {
        "risk": {
            "exposure": "data",
            "rebinned_exposed": [],
            "category_thresholds": [],
        }
    }

    def __init__(self, risk: str):
        """
        Parameters
        ----------
        risk :
            the type and name of a risk, specified as "type.name". Type is singular.
        """
        self.risk = EntityString(risk)
        self.configuration_defaults = self._get_configuration_defaults()
        self.exposure_distribution = self._get_exposure_distribution()
        self._sub_components = [self.exposure_distribution]

        self._randomness_stream_name = f"initial_{self.risk.name}_propensity"
        self.propensity_column_name = f"{self.risk.name}_propensity"
        self.propensity_pipeline_name = f"{self.risk.name}.propensity"
        self.gbd_exposure_pipeline_name = f"{self.risk.name}.gbd_exposure"
        self.exposure_pipeline_name = f"{self.risk.name}.exposure"

    def __repr__(self) -> str:
        return f"Risk({self.risk})"

    ##########################
    # Initialization methods #
    ##########################

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {self.risk.name: Risk.configuration_defaults["risk"]}

    def _get_exposure_distribution(self) -> SimulationDistribution:
        return SimulationDistribution(self.risk)

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return f"risk.{self.risk}"

    @property
    def sub_components(self) -> List:
        return self._sub_components

    #################
    # Setup methods #
    #################

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        self.randomness = self._get_randomness_stream(builder)
        self.propensity = self._get_propensity_pipeline(builder)
        self.gbd_exposure = self._get_gbd_exposure_pipeline(builder)
        self.exposure = self._get_exposure_pipeline(builder)
        self.population_view = self._get_population_view(builder)

        self._register_simulant_initializer(builder)

    def _get_randomness_stream(self, builder) -> RandomnessStream:
        return builder.randomness.get_stream(self._randomness_stream_name)

    def _get_propensity_pipeline(self, builder: Builder) -> Pipeline:
        return builder.value.register_value_producer(
            self.propensity_pipeline_name,
            source=lambda index: (
                self.population_view.subview([self.propensity_column_name])
                .get(index)
                .squeeze(axis=1)
            ),
            requires_columns=[self.propensity_column_name],
        )

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
            requires_values=[self._get_gbd_exposure_pipeline],
            preferred_post_processor=get_exposure_post_processor(builder, self.risk),
        )

    def _get_population_view(self, builder: Builder) -> PopulationView:
        return builder.population.get_view(
            [self.propensity_column_name, COLUMNS.SBP_MULTIPLIER]
        )

    def _register_simulant_initializer(self, builder: Builder) -> None:
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=[self.propensity_column_name],
            requires_streams=[self._randomness_stream_name],
        )

    ########################
    # Event-driven methods #
    ########################

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        self.population_view.update(
            pd.Series(
                self.randomness.get_draw(pop_data.index), name=self.propensity_column_name
            )
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
        # TODO: check that this is correct. At this point I seem to already have
        # a correct self.gbd_exposure as well as a self.exposure
        return (
            self.gbd_exposure(index) * self.population_view.get(index)[COLUMNS.SBP_MULTIPLIER]
        )
