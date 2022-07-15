from vivarium.framework.engine import Builder
from vivarium_public_health.metrics import ResultsStratifier as ResultsStratifier_


class ResultsStratifier(ResultsStratifier_):
    """Centralized component for handling results stratification.
    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.
    """

    def register_stratifications(self, builder: Builder) -> None:
        """Register each desired stratification with calls to _setup_stratification"""
        super().register_stratifications(builder)