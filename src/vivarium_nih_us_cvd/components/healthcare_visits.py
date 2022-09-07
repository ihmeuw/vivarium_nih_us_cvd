from typing import List, Dict, Tuple

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData
from vivarium.framework.randomness import get_hash
from vivarium.framework.time import Time


class HealthcareVisits:
    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__
    
    def __init__(self):
        pass

    def setup(self, builder: Builder) -> None:
        pass

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        pass

    def on_time_step_prepare(self, event: Event) -> None:
        pass

    def on_time_step(self, event: Event) -> None:
        pass

    def on_time_step_cleanup(self, event: Event) -> None:
        pass

    def on_collect_metrics(self, event: Event): -> None:
        pass
    
