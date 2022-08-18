from collections import Counter
from typing import Dict

from vivarium.framework.engine import Builder
from vivarium_public_health.metrics.stratification import (
    ResultsStratifier as ResultsStratifier_,
)
from vivarium_public_health.utilities import to_years

from vivarium_nih_us_cvd.constants.data_values import LDL_C_EXPOSURE_MAXIMUM, LDL_C_EXPOSURE_MINIMUM


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


class LdlcObserver:
    """Observes (continuous) LDL-Cholesterol exposure-time per group."""

    configuration_defaults = {
        "observers": {
            "ldl_c": {
                "exclude": [],
                "include": [],
            }
        }
    }

    def __repr__(self):
        return "LdlcObserver()"

    @property
    def name(self):
        return "ldl_c_observer"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.config = builder.configuration.observers.ldl_c
        self.stratifier = builder.components.get_component(ResultsStratifier.name)

        self.counter = Counter()

        self.ldlc = builder.value.get_value("high_ldl_cholesterol.exposure")

        columns_required = ["alive"]
        self.population_view = builder.population.get_view(columns_required)

        builder.event.register_listener("collect_metrics", self.on_collect_metrics)
        builder.value.register_value_modifier("metrics", self.metrics)

    def on_collect_metrics(self, event: "Event"):
        step_size_in_years = to_years(event.step_size)
        pop = self.population_view.get(event.index, query='alive == "alive"')
        ldlc_exposure = self.ldlc(pop.index)
        # FIXME: Hacky; this should be a proper test
        assert ldlc_exposure.min() >= LDL_C_EXPOSURE_MINIMUM
        assert ldlc_exposure.max() <= LDL_C_EXPOSURE_MAXIMUM

        new_observations = {}
        groups = self.stratifier.group(pop.index, self.config.include, self.config.exclude)
        for label, group_mask in groups:
            key = f"total_ldl_c_exposure_time_{label}"
            new_observations[key] = ldlc_exposure[group_mask].sum() * step_size_in_years

        self.counter.update(new_observations)

    def metrics(self, index: "pd.Index", metrics: Dict[str, float]) -> Dict[str, float]:
        metrics.update(self.counter)
        return metrics
