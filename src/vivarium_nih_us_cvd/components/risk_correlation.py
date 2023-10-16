from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import scipy
from vivarium import Component
from vivarium.framework.artifact import EntityKey
from vivarium.framework.engine import Builder
from vivarium.framework.lookup import LookupTable
from vivarium.framework.population.manager import SimulantData
from vivarium.framework.randomness import get_hash
from vivarium_public_health.utilities import EntityString

from vivarium_nih_us_cvd.components.risks import CorrelatedRisk
from vivarium_nih_us_cvd.constants import paths


class RiskCorrelation(Component):
    """Apply correlation to risk factor propensities. It also registers the
    PAF pipeline modifiers since with correlated risk factors we can no longer
    assume independent risks when calculating PAFs.
    """

    ##############
    # Properties #
    ##############

    @property
    def columns_created(self) -> List[str]:
        return self.propensity_column_names

    @property
    def columns_required(self) -> Optional[List[str]]:
        return ["age"]

    @property
    def initialization_requirements(self) -> Dict[str, List[str]]:
        return {"requires_columns": ["age"]}

    #####################
    # Lifecycle methods #
    #####################

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        self.risks = [
            EntityString(risk.name.replace("risk.", ""))
            for risk in builder.components.get_components_by_type(CorrelatedRisk)
        ]
        self.propensity_column_names = [f"{risk.name}_propensity" for risk in self.risks]

        self.input_draw = builder.configuration.input_data.input_draw_number
        self.random_seed = builder.configuration.randomness.random_seed
        self.correlation_data = pd.read_csv(paths.FILEPATHS.RISK_CORRELATION)
        self.population_attributable_fractions = (
            self.get_population_attributable_fraction_source(builder)
        )
        self.register_paf_modifiers(builder)

    #################
    # Setup methods #
    #################

    def get_population_attributable_fraction_source(
        self, builder: Builder
    ) -> Dict[str, LookupTable]:
        paf_data = builder.data.load(
            "risk_factor.joint_mediated_risks.population_attributable_fraction"
        )
        pafs = {}
        for (name, measure), group in paf_data.groupby(
            ["affected_entity", "affected_measure"]
        ):
            target = EntityKey(f"cause.{name}.{measure}")
            data = group.drop(columns=["affected_entity", "affected_measure"])
            pafs[target] = builder.lookup.build_table(
                data, key_columns=["sex"], parameter_columns=["age", "year"]
            )
        return pafs

    def register_paf_modifiers(self, builder: Builder) -> None:
        for target, pafs in self.population_attributable_fractions.items():
            target_paf_pipeline_name = f"{target.name}.{target.measure}.paf"
            builder.value.register_value_modifier(
                target_paf_pipeline_name,
                modifier=pafs,
                requires_columns=["age", "sex"],
            )

    ########################
    # Event-driven methods #
    ########################

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        pop = self.population_view.subview(["age"]).get(pop_data.index)
        propensities = pd.DataFrame(index=pop.index)

        correlation = self.update_correlation_data(self.correlation_data)

        for i in range(len(correlation)):
            age_start = correlation.iloc[i]["age_start"]
            age_end = correlation.iloc[i]["age_end"]

            age_specific_pop = pop.query("age >= @age_start and age < @age_end")
            covariance_matrix = [
                [
                    correlation.iloc[i][f"{first_risk.name}_AND_{second_risk.name}"]
                    for second_risk in self.risks
                ]
                for first_risk in self.risks
            ]

            np.random.seed(get_hash(f"{self.input_draw}_{self.random_seed}"))
            probit_propensity = np.random.multivariate_normal(
                mean=[0] * len(self.risks), cov=covariance_matrix, size=len(age_specific_pop)
            )
            correlated_propensities = scipy.stats.norm().cdf(probit_propensity)
            propensities.loc[
                age_specific_pop.index, self.propensity_column_names
            ] = correlated_propensities

        self.population_view.update(propensities)

    ##################
    # Helper methods #
    ##################

    def update_correlation_data(self, correlation_data: pd.DataFrame) -> pd.DataFrame:
        """Add correlations of 1 for risks with themselves and add columns with names
        of risk factor pairs switched. This makes creating the covariance matrix much cleaner.
        """

        # risks are perfectly correlated with themselves
        for risk in self.risks:
            correlation_data[f"{risk.name}_AND_{risk.name}"] = 1

        # add columns with risk pairs switched in column name
        risk_pairs = [col for col in correlation_data.columns if "AND" in col]
        switched_risk_pairs = [
            pair.split("_AND_")[1] + "_AND_" + pair.split("_AND_")[0] for pair in risk_pairs
        ]

        for original_column, new_column in tuple(zip(risk_pairs, switched_risk_pairs)):
            correlation_data[new_column] = correlation_data[original_column].values

        return correlation_data
