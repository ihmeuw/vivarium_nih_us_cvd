from vivarium.framework.engine import Builder
from vivarium_public_health.metrics.stratification import ResultsStratifier as ResultsStratifier_


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
        age_bins = age_bins[age_bins['age_start'] >= 25.0].reset_index(drop=True)
        age_bins.loc[len(age_bins.index)] = [7.0, 25.0, '7_to_24']
        self.age_bins = age_bins.sort_values(['age_start']).reset_index(drop=True)
        self.register_stratifications(builder)
