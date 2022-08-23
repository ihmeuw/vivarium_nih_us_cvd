from collections import Counter
from typing import Dict

from vivarium.framework.engine import Builder
from vivarium_public_health.metrics.stratification import (
    ResultsStratifier as ResultsStratifier_,
)
from vivarium_public_health.utilities import EntityString, to_years

from vivarium_nih_us_cvd.constants.data_values import RISK_EXPOSURE_LIMITS


class ResultsStratifier(ResultsStratifier_):
    """Centralized component for handling results stratification.
    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.
    """

    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        age_bins = self.age_bins
        age_bins = age_bins[age_bins["age_start"] >= 25.0].reset_index(drop=True)
        age_bins.loc[len(age_bins.index)] = [7.0, 25.0, "7_to_24"]
        self.age_bins = age_bins.sort_values(["age_start"]).reset_index(drop=True)
        self.register_stratifications(builder)


class RiskExposureTimeObserver:
    """Observes (continuous) risk exposure-time per group."""

    configuration_defaults = {
        "observers": {
            "risk": {
                "exclude": [],
                "include": [],
            }
        }
    }

    def __init__(self, risk: str):
        self.risk = self.risk = EntityString(risk)
        self.configuration_defaults = self._get_configuration_defaults()

    def __repr__(self):
        return f"RiskExposureTimeObserver({self.risk})"

    ##########################
    # Initialization methods #
    ##########################

    # noinspection PyMethodMayBeStatic
    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "observers": {
                self.risk: RiskExposureTimeObserver.configuration_defaults["observers"]["risk"]
            }
        }

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"risk_exposure_time_observer.{self.risk}"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.config = self._get_stratification_configuration(builder)
        self.stratifier = builder.components.get_component(ResultsStratifier.name)

        self.counter = Counter()

        self.exposure = builder.value.get_value(f"{self.risk.name}.exposure")

        columns_required = ["alive"]
        self.population_view = builder.population.get_view(columns_required)

        builder.event.register_listener("collect_metrics", self.on_collect_metrics)
        builder.value.register_value_modifier("metrics", self.metrics)

    def _get_stratification_configuration(self, builder: Builder) -> "ConfigTree":
        return builder.configuration.observers[self.risk]

    def on_collect_metrics(self, event: "Event"):
        step_size_in_years = to_years(event.step_size)
        pop = self.population_view.get(event.index, query='alive == "alive"')
        values = self.exposure(pop.index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            assert values.min() >= RISK_EXPOSURE_LIMITS[self.risk.name]["minimum"]
            assert values.max() <= RISK_EXPOSURE_LIMITS[self.risk.name]["maximum"]

        new_observations = {}
        groups = self.stratifier.group(pop.index, self.config.include, self.config.exclude)
        for label, group_mask in groups:
            key = f"total_risk_exposure_time_{self.risk.name}_{label}"
            new_observations[key] = values[group_mask].sum() * step_size_in_years

        self.counter.update(new_observations)

    def metrics(self, index: "pd.Index", metrics: Dict[str, float]) -> Dict[str, float]:
        metrics.update(self.counter)
        return metrics
