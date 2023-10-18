from typing import Any, Dict, List, Optional, Set

import pandas as pd
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium_public_health.metrics.stratification import (
    ResultsStratifier as ResultsStratifier_,
)
from vivarium_public_health.utilities import EntityString, TargetString, to_years

from vivarium_nih_us_cvd.components.effects import MEDIATOR_NAMES
from vivarium_nih_us_cvd.constants import data_values


class SimpleResultsStratifier(ResultsStratifier_):
    """Centralized component for handling results stratification.
    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.
    """

    ###########
    # Mappers #
    ###########

    def get_age_bins(self, builder: Builder) -> pd.DataFrame:
        """Re-define youngest age bin to 5_to_24"""
        age_bins = super().get_age_bins(builder)
        age_bins = age_bins[age_bins["age_start"] >= 25.0].reset_index(drop=True)
        age_bins.loc[len(age_bins.index)] = [5.0, 25.0, "5_to_24"]

        # FIXME: MIC-4083 simulants can age past 125
        max_age = age_bins["age_end"].max()
        age_bins.loc[age_bins["age_end"] == max_age, "age_end"] += (
            builder.configuration.time.step_size / 365.25
        )

        return age_bins.sort_values(["age_start"]).reset_index(drop=True)


class ResultsStratifier(SimpleResultsStratifier):
    """Centralized component for handling results stratification.
    This should be used as a sub-component for observers.  The observers
    can then ask this component for population subgroups and labels during
    results production and have this component manage adjustments to the
    final column labels for the subgroups.
    """

    #################
    # Setup methods #
    #################

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


class ContinuousRiskObserver(Component):
    """Observes (continuous) risk exposure-time per group."""

    CONFIGURATION_DEFAULTS = {
        "stratification": {
            "risk": {
                "exclude": [],
                "include": [],
            }
        }
    }

    ##############
    # Properties #
    ##############

    @property
    def configuration_defaults(self) -> Dict[str, Any]:
        return {
            "stratification": {
                self.risk: self.CONFIGURATION_DEFAULTS["stratification"]["risk"]
            }
        }

    @property
    def columns_required(self) -> Optional[List[str]]:
        return ["alive"]

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self, risk: str):
        super().__init__()
        self.risk = EntityString(risk)

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[self.risk]

        builder.results.register_observation(
            name=f"total_exposure_time_risk_{self.risk.name}",
            pop_filter='alive=="alive" and tracked==True',
            aggregator=self.aggregate_state_person_time,
            requires_columns=["alive"],
            requires_values=[f"{self.risk.name}.exposure"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="collect_metrics",
        )

    ###############
    # Aggregators #
    ###############

    def aggregate_state_person_time(self, x: pd.DataFrame) -> float:
        return sum(x[f"{self.risk.name}.exposure"]) * to_years(self.step_size())


class HealthcareVisitObserver(Component):
    """Observes doctor visit counts per group."""

    CONFIGURATION_DEFAULTS = {
        "stratification": {
            "visits": {
                "exclude": [],
                "include": [],
            }
        }
    }

    ##############
    # Properties #
    ##############

    @property
    def columns_required(self) -> Optional[List[str]]:
        return [data_values.COLUMNS.VISIT_TYPE]

    #####################
    # Lifecycle methods #
    #####################

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification["visits"]

        for visit_type in data_values.VISIT_TYPE:
            builder.results.register_observation(
                name=f"healthcare_visits_{visit_type}",
                pop_filter=(
                    'alive=="alive" and tracked==True and '
                    f'visit_type=="{visit_type}"'
                ),
                requires_columns=["alive", data_values.COLUMNS.VISIT_TYPE],
                additional_stratifications=self.config.include,
                excluded_stratifications=self.config.exclude,
                when="collect_metrics",
            )


class CategoricalColumnObserver(Component):
    """Observes person-time of a categorical state table column"""

    CONFIGURATION_DEFAULTS = {
        "stratification": {
            "column": {
                "exclude": [],
                "include": [],
            }
        }
    }

    ##############
    # Properties #
    ##############

    @property
    def configuration_defaults(self) -> Dict[str, Any]:
        return {
            "stratification": {
                f"{self.column}": self.CONFIGURATION_DEFAULTS["stratification"]["column"]
            }
        }

    @property
    def columns_required(self) -> Optional[List[str]]:
        return ["alive", self.column]

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self, column: str):
        super().__init__()
        self.column = column

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[self.column]
        self.categories = self.get_categories()

        self.register_observations(builder)

    #################
    # Setup methods #
    #################

    def register_observations(self, builder: Builder) -> None:
        for category in self.categories:
            builder.results.register_observation(
                name=f"{self.column}_{category}_person_time",
                pop_filter=(
                    'alive=="alive" and tracked==True and '
                    f'{self.column}=="{category}"'
                ),
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

    ###############
    # Aggregators #
    ###############

    def calculate_categorical_person_time(self, x: pd.DataFrame) -> float:
        return len(x) * to_years(self.step_size())


class LifestyleObserver(CategoricalColumnObserver):
    #####################
    # Lifecycle methods #
    #####################
    def __init__(self):
        super().__init__("lifestyle")

    #################
    # Setup methods #
    #################

    def register_observations(self, builder: Builder) -> None:
        builder.results.register_observation(
            name=f"lifestyle_cat1_person_time",
            pop_filter='alive=="alive" and tracked==True',
            aggregator=self.calculate_exposed_lifestyle_person_time,
            requires_columns=["alive", self.column],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="time_step__prepare",
        )
        builder.results.register_observation(
            name=f"lifestyle_cat2_person_time",
            pop_filter='alive=="alive" and tracked==True',
            aggregator=self.calculate_unexposed_lifestyle_person_time,
            requires_columns=["alive", self.column],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="time_step__prepare",
        )

    def get_categories(self) -> List[str]:
        return ["cat1", "cat2"]

    ###############
    # Aggregators #
    ###############

    def calculate_exposed_lifestyle_person_time(self, x: pd.DataFrame) -> float:
        return sum(~(x["lifestyle"].isna())) * to_years(self.step_size())

    def calculate_unexposed_lifestyle_person_time(self, x: pd.DataFrame) -> float:
        return sum(x["lifestyle"].isna()) * to_years(self.step_size())


class BinnedRiskObserver(Component):
    """Observes (continuous) risk exposure-time per group binned by exposure thresholds."""

    CONFIGURATION_DEFAULTS = {
        "stratification": {
            "risk": {
                "exclude": [],
                "include": [],
            }
        }
    }

    ##############
    # Properties #
    ##############

    @property
    def configuration_defaults(self) -> Dict[str, Any]:
        return {
            "stratification": {
                f"binned_{self.risk}": self.CONFIGURATION_DEFAULTS["stratification"]["risk"]
            }
        }

    @property
    def columns_required(self) -> Optional[List[str]]:
        return ["alive"]

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self, risk: str):
        super().__init__()
        self.risk = EntityString(risk)

    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification[f"binned_{self.risk}"]

        try:
            thresholds = data_values.BINNED_OBSERVER_THRESHOLDS[self.risk.name]
        except:
            raise ValueError(
                "Thresholds only defined for high_ldl_cholesterol and high_systolic_blood_pressure."
                f"You provided {self.risk}."
            )

        builder.results.register_observation(
            name=f"total_exposure_time_risk_{self.risk.name}_below_{thresholds[0]}",
            pop_filter=(
                'alive=="alive" and tracked==True and '
                f'`{self.risk.name}.exposure`<{thresholds[0]}'
            ),
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
                    'alive=="alive" and tracked==True and '
                    f'`{self.risk.name}.exposure`>={thresholds[left_threshold_idx]} and '
                    f'`{self.risk.name}.exposure`<{thresholds[left_threshold_idx+1]}'
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
            pop_filter=(
                'alive=="alive" and tracked==True and '
                f'`{self.risk.name}.exposure`>={thresholds[len(thresholds)-1]}'
            ),
            aggregator=self.aggregate_state_person_time,
            requires_columns=["alive"],
            requires_values=[f"{self.risk.name}.exposure"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="collect_metrics",
        )

    ###############
    # Aggregators #
    ###############

    def aggregate_state_person_time(self, x: pd.DataFrame) -> float:
        return len(x) * to_years(self.step_size())


class JointPAFObserver(Component):
    CONFIGURATION_DEFAULTS = {
        "stratification": {
            "paf": {
                "exclude": [],
                "include": [],
            }
        }
    }

    @property
    def configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "stratification": {
                f"joint_paf_on_{self.target.name}": self.CONFIGURATION_DEFAULTS[
                    "stratification"
                ]["paf"]
            }
        }

    def __init__(self, target: str):
        super().__init__()
        self.target = TargetString(target)
        self.risks_and_mediators = self._get_risks_and_mediators()

    def _get_risks_and_mediators(self) -> Set[str]:
        """MEDIATOR_NAMES is a nested dict like {risk: {target: [mediators]}}.
        For this instance target (the second-level keys), extract the
        risks (the outer keys) and the mediator names (the child node lists).
        """
        risks = {
            risk
            for risk, mediator_dict in MEDIATOR_NAMES.items()
            if self.target.name in mediator_dict.keys()
        }
        mediator_lists = [MEDIATOR_NAMES[risk][self.target.name] for risk in risks]
        return risks.union(
            {mediator for mediators in mediator_lists for mediator in mediators}
        )

    def setup(self, builder: Builder) -> None:
        self.risk_effects = {
            risk: builder.components.get_component(
                f"paf_calculation_risk_effect.risk_factor.{risk}.{self.target}"
            )
            for risk in self.risks_and_mediators
        }
        config = builder.configuration.stratification[f"joint_paf_on_{self.target.name}"]
        builder.results.register_observation(
            name=f"joint_paf_on_{self.target}",
            pop_filter='alive=="alive" and tracked==True',
            aggregator=self.calculate_paf,
            requires_columns=["alive"],
            requires_values=[
                f"unadjusted_rr_{x}_on_{self.target.name}" for x in self.risks_and_mediators
            ],
            additional_stratifications=config.include,
            excluded_stratifications=config.exclude,
            when="time_step__prepare",
        )

    def calculate_paf(self, x: pd.DataFrame) -> float:
        joint_rrs = pd.Series(1.0, index=x.index)
        for risk in self.risk_effects:
            joint_rrs = self.risk_effects[risk].mediated_target_modifier(x.index, joint_rrs)
        mean_rr = joint_rrs.mean()
        paf = (mean_rr - 1.0) / mean_rr
        return paf
