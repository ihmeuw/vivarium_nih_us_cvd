import numpy as np
import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.population import SimulantData
from vivarium_public_health.population import BasePopulation

from vivarium_nih_us_cvd.constants import data_keys


class EvenlyDistributedPopulation(BasePopulation):
    """
    Component for producing and aging simulants which are initialized with ages
    evenly distributed between age start and age end, and evenly split between
    male and female.
    """

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        super().setup(builder)
        self.location = builder.data.load(data_keys.POPULATION.LOCATION)

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        age_start = pop_data.user_data.get("age_start", self.config.age_start)
        age_end = pop_data.user_data.get("age_end", self.config.age_end)

        population = pd.DataFrame(index=pop_data.index)
        population["entrance_time"] = pop_data.creation_time
        population["exit_time"] = pd.NaT
        population["alive"] = "alive"
        population["location"] = self.location
        population["age"] = np.linspace(
            age_start, age_end, num=len(population) + 1, endpoint=False
        )[1:]
        population["sex"] = "Female"
        population.loc[population.index % 2 == 1, "sex"] = "Male"
        self.register_simulants(population[list(self.key_columns)])
        self.population_view.update(population)
