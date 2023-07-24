import numpy as np
import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.population.manager import SimulantData
from vivarium_public_health.utilities import EntityString

from vivarium_nih_us_cvd.components.risks import CorrelatedRisk
from vivarium_nih_us_cvd.constants import paths


class RiskCorrelation:
    """Apply correlation to risk factor propensities"""

    def __repr__(self) -> str:
        return f"RiskCorrelation"

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return f"risk_correlation"

    #################
    # Setup methods #
    #################

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        self.correlation_data = pd.read_csv(paths.FILEPATHS.RISK_CORRELATION)
        self.risks = [
            EntityString(risk.name.replace("risk.", ""))
            for risk in builder.components.get_components_by_type(CorrelatedRisk)
        ]
        self.propensity_column_names = [f"{risk.name}_propensity" for risk in self.risks]
        self.population_view = builder.population.get_view(
            ["age"] + self.propensity_column_names
        )

        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=self.propensity_column_names,
            requires_columns=["age"],
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
        of risk factor pairs switched. This makes creating the covariance matrix much cleaner."""

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
