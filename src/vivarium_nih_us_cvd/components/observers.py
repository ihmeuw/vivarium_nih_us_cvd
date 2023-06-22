from collections import Counter
from typing import Dict, List

import pandas as pd
from vivarium.config_tree import ConfigTree
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import PopulationView
from vivarium.framework.time import get_time_stamp
from vivarium_public_health.metrics.stratification import (
    ResultsStratifier as ResultsStratifier_,
)
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

    def get_age_bins(self, builder: Builder) -> pd.DataFrame:
        """Re-define youngest age bin to 5_to_24"""
        age_bins = super().get_age_bins(builder)
        age_bins = age_bins[age_bins["age_start"] >= 25.0].reset_index(drop=True)
        age_bins.loc[len(age_bins.index)] = [5.0, 25.0, "5_to_24"]

        return age_bins.sort_values(["age_start"]).reset_index(drop=True)

    def register_stratifications(self, builder: Builder) -> None:
        super().register_stratifications(builder)

        builder.results.register_stratification(
            name=data_values.COLUMNS.SBP_MEDICATION_ADHERENCE,
            categories=[level for level in data_values.MEDICATION_ADHERENCE_TYPE],
            is_vectorized=True,
            requires_columns=[data_values.COLUMNS.SBP_MEDICATION_ADHERENCE],
        )
        builder.results.register_stratification(
            name=data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE,
            categories=[level for level in data_values.MEDICATION_ADHERENCE_TYPE],
            is_vectorized=True,
            requires_columns=[data_values.COLUMNS.LDLC_MEDICATION_ADHERENCE],
        )


class ContinuousRiskObserver:
    """Observes (continuous) risk exposure-time per group."""

    configuration_defaults = {
        "stratification": {
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
            "stratification": {
                self.risk: ContinuousRiskObserver.configuration_defaults["stratification"][
                    "risk"
                ]
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
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[self.risk]

        columns_required = ["alive"]
        self.population_view = builder.population.get_view(columns_required)

        builder.results.register_observation(
            name=f"total_exposure_time_risk_{self.risk.name}",
            pop_filter=f'alive == "alive"',
            aggregator=self.aggregate_state_person_time,
            requires_columns=["alive"],
            requires_values=[f"{self.risk.name}.exposure"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="collect_metrics",
        )

    def aggregate_state_person_time(self, x: pd.DataFrame) -> float:
        return sum(x[f"{self.risk.name}.exposure"]) * to_years(self.step_size())


class HealthcareVisitObserver:
    """Observes doctor visit counts per group."""

    configuration_defaults = {
        "stratification": {
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
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification["visits"]

        columns_required = [data_values.COLUMNS.VISIT_TYPE]
        self.population_view = builder.population.get_view(columns_required)

        for visit_type in data_values.VISIT_TYPE:
            builder.results.register_observation(
                name=f"healthcare_visits_{visit_type}",
                pop_filter=f'alive == "alive" and visit_type == "{visit_type}"',
                requires_columns=["alive", data_values.COLUMNS.VISIT_TYPE],
                additional_stratifications=self.config.include,
                excluded_stratifications=self.config.exclude,
                when="collect_metrics",
            )


class CategoricalColumnObserver:
    """Observes person-time of a categorical state table column"""

    configuration_defaults = {
        "stratification": {
            "column": {
                "exclude": [],
                "include": [],
            }
        }
    }

    def __init__(self, column):
        self.column = column
        self.configuration_defaults = self._get_configuration_defaults()

    def __repr__(self):
        return f"PersonTimeObserver({self.column})"

    ##########################
    # Initialization methods #
    ##########################

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "stratification": {
                f"{self.column}": CategoricalColumnObserver.configuration_defaults[
                    "stratification"
                ]["column"]
            }
        }

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"person_time_observer.{self.column}"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[self.column]
        self.categories = self.get_categories()

        columns_required = ["alive", self.column]
        self.population_view = builder.population.get_view(columns_required)

        self.register_observations(builder)

    def register_observations(self, builder: Builder) -> None:
        for category in self.categories:
            builder.results.register_observation(
                name=f"{self.column}_{category}_person_time",
                pop_filter=f'alive == "alive" and {self.column} == "{category}"',
                aggregator=self.calculate_categorical_person_time,
                requires_columns=["alive", self.column],
                additional_stratifications=self.config.include,
                excluded_stratifications=self.config.exclude,
                when="time_step__prepare",
            )

    def get_categories(self) -> List[str]:
        mapping = {
            data_values.COLUMNS.SBP_MEDICATION: [
                level.DESCRIPTION for level in data_values.SBP_MEDICATION_LEVEL
            ],
            data_values.COLUMNS.LDLC_MEDICATION: [
                level.DESCRIPTION for level in data_values.LDLC_MEDICATION_LEVEL
            ],
            data_values.COLUMNS.OUTREACH: list(data_values.INTERVENTION_CATEGORY_MAPPING),
            data_values.COLUMNS.POLYPILL: list(data_values.INTERVENTION_CATEGORY_MAPPING),
        }

        return mapping[self.column]

    def calculate_categorical_person_time(self, x: pd.DataFrame) -> float:
        return len(x) * to_years(self.step_size())


class LifestyleObserver(CategoricalColumnObserver):
    def __init__(self):
        self.column = "lifestyle"
        self.configuration_defaults = self._get_configuration_defaults()

    def register_observations(self, builder: Builder) -> None:
        builder.results.register_observation(
            name=f"lifestyle_cat1_person_time",
            pop_filter=f'alive == "alive"',
            aggregator=self.calculate_exposed_lifestyle_person_time,
            requires_columns=["alive", self.column],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="time_step__prepare",
        )
        builder.results.register_observation(
            name=f"lifestyle_cat2_person_time",
            pop_filter=f'alive == "alive"',
            aggregator=self.calculate_unexposed_lifestyle_person_time,
            requires_columns=["alive", self.column],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="time_step__prepare",
        )

    def get_categories(self) -> List[str]:
        return ["cat1", "cat2"]

    def calculate_exposed_lifestyle_person_time(self, x: pd.DataFrame) -> float:
        return sum(~(x["lifestyle"].isna())) * to_years(self.step_size())

    def calculate_unexposed_lifestyle_person_time(self, x: pd.DataFrame) -> float:
        return sum(x["lifestyle"].isna()) * to_years(self.step_size())


class BinnedRiskObserver:
    """Observes (continuous) risk exposure-time per group binned by exposure thresholds."""

    configuration_defaults = {
        "stratification": {
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
        return f"BinnedRiskObserver({self.risk})"

    ##########################
    # Initialization methods #
    ##########################

    # noinspection PyMethodMayBeStatic
    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "stratification": {
                f"binned_{self.risk}": ContinuousRiskObserver.configuration_defaults[
                    "stratification"
                ]["risk"]
            }
        }

    ##############
    # Properties #
    ##############

    @property
    def name(self):
        return f"binned_risk_observer.{self.risk}"

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[f"binned_{self.risk}"]

        columns_required = ["alive"]
        self.population_view = builder.population.get_view(columns_required)

        if self.risk.name == "high_ldl_cholesterol":
            thresholds = data_values.BINNED_OBSERVER_THRESHOLDS.LDL_THRESHOLDS
        elif self.risk.name == "high_systolic_blood_pressure":
            thresholds = data_values.BINNED_OBSERVER_THRESHOLDS.SBP_THRESHOLDS
        else:
            raise ValueError(
                "Thresholds only defined for high_ldl_cholesterol and high_systolic_blood_pressure."
                f"You provided {self.risk}."
            )

        builder.results.register_observation(
            name=f"total_exposure_time_risk_{self.risk.name}_below_{thresholds[0]}",
            pop_filter=f'alive == "alive" and `{self.risk.name}.exposure` < {thresholds[0]}',
            aggregator=self.aggregate_state_person_time,
            requires_columns=["alive"],
            requires_values=[f"{self.risk.name}.exposure"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="collect_metrics",
        )

        for left_threshold_idx in range(0, len(thresholds) - 1):
            builder.results.register_observation(
                name=(
                    f"total_exposure_time_risk_{self.risk.name}"
                    f"_between_{thresholds[left_threshold_idx]}_and_{thresholds[left_threshold_idx+1]}"
                ),
                pop_filter=(
                    f'alive == "alive" and '
                    f"`{self.risk.name}.exposure` >= {thresholds[left_threshold_idx]} "
                    f"and `{self.risk.name}.exposure` < {thresholds[left_threshold_idx+1]}"
                ),
                aggregator=self.aggregate_state_person_time,
                requires_columns=["alive"],
                requires_values=[f"{self.risk.name}.exposure"],
                additional_stratifications=self.config.include,
                excluded_stratifications=self.config.exclude,
                when="collect_metrics",
            )

        builder.results.register_observation(
            name=f"total_exposure_time_risk_{self.risk.name}_above_{thresholds[len(thresholds)-1]}",
            pop_filter=f'alive == "alive" and `{self.risk.name}.exposure` >= {thresholds[len(thresholds)-1]}',
            aggregator=self.aggregate_state_person_time,
            requires_columns=["alive"],
            requires_values=[f"{self.risk.name}.exposure"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="collect_metrics",
        )

    def aggregate_state_person_time(self, x: pd.DataFrame) -> float:
        return len(x) * to_years(self.step_size())
