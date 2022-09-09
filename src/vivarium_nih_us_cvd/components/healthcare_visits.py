from typing import List, Dict, Tuple

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData
from vivarium.framework.randomness import get_hash
from vivarium.framework.time import Time

from vivarium_nih_us_cvd.constants import data_keys, models
from vivarium_nih_us_cvd.constants.data_values import VISIT_TYPE


class HealthcareVisits:
    """ Manages healthcare utilization """
    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__
    
    def __init__(self):
        pass

    def __repr__(self):
        return "HealthcareVisits"


    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)
        
        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(utilization_data,
                                                                 parameter_columns=['age', 'year'],
                                                                 key_columns=['sex'])
        self.background_utilization_rate = builder.value.register_rate_producer('utilization_rate',
                                                                                background_utilization_rate,
                                                                                requires_columns=['age', 'sex'])

        # Add columns
        columns_created = [VISIT_TYPE.name, VISIT_TYPE.SCHEDULED_COLUMN_NAME]
        columns_required = [models.ISCHEMIC_STROKE_MODEL_NAME, models.MYOCARDIAL_INFARCTION_MODEL_NAME]
        self.population_view = builder.population.get_view(columns_required + columns_created)

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
        )

        # Register listeners
        builder.event.register_listener('time_step__cleanup', self.on_time_step_cleanup)
        builder.event.register_listener('time_step', self.on_time_step)


    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        pass


    def on_time_step(self, event: Event) -> None:
        pass


    def on_time_step_cleanup(self, event: Event) -> None:
        pass
