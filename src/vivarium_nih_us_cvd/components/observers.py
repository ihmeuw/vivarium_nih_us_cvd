from collections import Counter
from typing import Dict, List

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.time import get_time_stamp
from vivarium_public_health.metrics.stratification import (
    ResultsStratifier as ResultsStratifier_,
)
from vivarium_public_health.metrics.stratification import Source, SourceType
from vivarium_public_health.utilities import EntityString, to_years

from vivarium_nih_us_cvd.constants import data_values


class ResultsStratifier(ResultsStratifier_):
    """Centralized component for handling results stratification.
    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.
    """

    def setup(self, builder: Builder) -> None:
        super().setup(builder)

    def _get_age_bins(self, builder: Builder) -> pd.DataFrame:
        """Re-define youngest age bin to 7_to_24"""
        age_bins = super()._get_age_bins(builder)
        age_bins = age_bins[age_bins["age_start"] >= 25.0].reset_index(drop=True)
        age_bins.loc[len(age_bins.index)] = [7.0, 25.0, "7_to_24"]

        return age_bins.sort_values(["age_start"]).reset_index(drop=True)

    def register_stratifications(self, builder: Builder) -> None:
        super().register_stratifications(builder)

        self.setup_stratification(
            builder,
            name=data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            sources=[Source(data_values.COLUMNS.SBP_MEDICATION_ADHERENCE, SourceType.COLUMN)],
            categories={level for level in data_values.MEDICATION_ADHERENCE_TYPE},
        )

        self.setup_stratification(
            builder,
            name=data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
            sources=[Source(data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE, SourceType.COLUMN)],
            categories={level for level in data_values.MEDICATION_ADHERENCE_TYPE},
        )


class ContinuousRiskObserver:
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
        self.risk = EntityString(risk)
        self.configuration_defaults = self._get_configuration_defaults()

    def __repr__(self):
        return f"ContinuousRiskObserver({self.risk})"

    ##########################
    # Initialization methods #
    ##########################

    # noinspection PyMethodMayBeStatic
    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "observers": {
                self.risk: ContinuousRiskObserver.configuration_defaults["observers"]["risk"]
            }
        }

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"continuous_risk_observer.{self.risk}"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.observation_start_time = get_time_stamp(
            builder.configuration.time.observation_start
        )
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

    def on_collect_metrics(self, event: Event):
        if event.time < self.observation_start_time:
            return
        step_size_in_years = to_years(event.step_size)
        pop = self.population_view.get(event.index, query='alive == "alive"')
        values = self.exposure(pop.index)

        new_observations = {}
        groups = self.stratifier.group(pop.index, self.config.include, self.config.exclude)
        for label, group_mask in groups:
            key = f"total_exposure_time_risk_{self.risk.name}_{label}"
            new_observations[key] = values[group_mask].sum() * step_size_in_years

        self.counter.update(new_observations)

    def metrics(self, index: "pd.Index", metrics: Dict[str, float]) -> Dict[str, float]:
        metrics.update(self.counter)
        return metrics


class HealthcareVisitObserver:
    """Observes doctor visit counts per group."""

    configuration_defaults = {
        "observers": {
            "visits": {
                "exclude": [],
                "include": [],
            }
        }
    }

    def __init__(self):
        self.configuration_defaults = self._get_configuration_defaults()

    def __repr__(self):
        return f"HealthcareVisitObserver"

    ##########################
    # Initialization methods #
    ##########################

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return HealthcareVisitObserver.configuration_defaults

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"healthcare_visit_observer"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.observation_start_time = get_time_stamp(
            builder.configuration.time.observation_start
        )
        self.config = self._get_stratification_configuration(builder)
        self.stratifier = builder.components.get_component(ResultsStratifier.name)

        self.counter = Counter()

        columns_required = [data_values.COLUMNS.VISIT_TYPE]
        self.population_view = builder.population.get_view(columns_required)

        builder.event.register_listener("collect_metrics", self.on_collect_metrics)
        builder.value.register_value_modifier("metrics", self.metrics)

    def _get_stratification_configuration(self, builder: Builder) -> "ConfigTree":
        return builder.configuration.observers["visits"]

    def on_collect_metrics(self, event: Event):
        if event.time < self.observation_start_time:
            return
        pop = self.population_view.get(event.index, query='alive == "alive"')

        new_observations = {}
        groups = self.stratifier.group(pop.index, self.config.include, self.config.exclude)
        for label, group_mask in groups:
            for visit_type in data_values.VISIT_TYPE:
                key = f"healthcare_visits_{visit_type}_{label}"
                new_observations[key] = sum(pop[group_mask].squeeze() == visit_type)
        self.counter.update(new_observations)

    def metrics(self, index: "pd.Index", metrics: Dict[str, float]) -> Dict[str, float]:
        metrics.update(self.counter)
        return metrics


class MedicationObserver:
    """Observes person-time on medication"""

    configuration_defaults = {
        "observers": {
            "medication": {
                "exclude": [],
                "include": [],
            },
        },
    }

    def __init__(self, risk):
        self.medication_type = self._get_medication_type(risk)
        self.medication_levels = self._get_medication_levels(risk)
        self.configuration_defaults = self._get_configuration_defaults()

    def __repr__(self):
        return f"MedicationObserver({self.medication_type})"

    ##########################
    # Initialization methods #
    ##########################

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "observers": {
                self.medication_type: MedicationObserver.configuration_defaults["observers"]["medication"]
            }
        }

    def _get_medication_type(self, risk: str) -> str:
        mapping = {
            "risk_factor.high_systolic_blood_pressure": data_values.COLUMNS.SBP_MEDICATION,
            "risk_factor.high_ldl_cholesterol": data_values.COLUMNS.LDLC_MEDICATION,
        }

        return mapping[risk]

    def _get_medication_levels(self, risk: str) -> List[str]:
        mapping = {
            "risk_factor.high_systolic_blood_pressure": data_values.SBP_MEDICATION_LEVEL,
            "risk_factor.high_ldl_cholesterol": data_values.LDLC_MEDICATION_LEVEL,
        }

        return [level.DESCRIPTION for level in mapping[risk]]

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"medication_observer.{self.medication_type}"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.observation_start_time = get_time_stamp(
            builder.configuration.time.observation_start
        )
        self.config = self._get_stratification_configuration(builder)
        self.stratifier = builder.components.get_component(ResultsStratifier.name)
        columns_required = ["alive", self.medication_type]
        self.population_view = builder.population.get_view(columns_required)

        self.counter = Counter()

        # The medications get updated at the end of the time step and so we want
        # to observe the time on each medication at the beginning of each time
        # step before any changes are made
        builder.event.register_listener("time_step__prepare", self.on_time_step_prepare)
        builder.value.register_value_modifier("metrics", self.metrics)

    def _get_stratification_configuration(self, builder: Builder) -> "ConfigTree":
        return builder.configuration.observers[self.medication_type]

    def on_time_step_prepare(self, event: Event):
        if event.time < self.observation_start_time:
            return
        step_size_in_years = to_years(event.step_size)
        medications = self.population_view.get(event.index, query='alive == "alive"')[
            self.medication_type
        ]

        groups = self.stratifier.group(
            medications.index, self.config.include, self.config.exclude
        )
        new_observations = {}
        for label, group_mask in groups:
            for med in self.medication_levels:
                med_mask = medications == med
                key = f"{self.medication_type}_person_time_{med}_{label}"
                new_observations[key] = (
                    len(medications[group_mask & med_mask]) * step_size_in_years
                )

        self.counter.update(new_observations)

    def metrics(self, index: "pd.Index", metrics: Dict[str, float]) -> Dict[str, float]:
        metrics.update(self.counter)
        return metrics
