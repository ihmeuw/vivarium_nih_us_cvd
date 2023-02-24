from typing import Dict, List

import pandas as pd
from vivarium.framework.engine import Builder


class PAFObserver:

    configuration_defaults = {
        "stratification": {
            "paf": {
                "exclude": [],
                "include": [],
            }
        }
    }

    def __init__(self, risk: str, target: str):
        self.risk = risk
        self.target = target
        self.configuration_defaults = self.get_configuration_defaults()

    def __repr__(self):
        return f"PAFObserver({self.risk}, {self.target})"

    @property
    def name(self):
        return f"paf_observer.{self.risk}.{self.target}"

    # noinspection PyAttributeOutsideInit
    def setup(self, builder: Builder) -> None:
        self.step_size = builder.time.step_size()
        self.config = builder.configuration.stratification["paf"]
        self.risk_effect = builder.components.get_component(f"risk_effect.{self.risk}.{self.target}")

        builder.results.register_observation(
            name=f"calculated_paf_{self.risk}_on_{self.target}",
            pop_filter=f'alive == "alive"',
            aggregator=self.calculate_paf,
            requires_columns=["alive"],
            additional_stratifications=self.config.include,
            excluded_stratifications=self.config.exclude,
            when="time_step__prepare",
        )

    def calculate_paf(self, x: pd.DataFrame) -> float:
        relative_risk = self.risk_effect.target_modifier(x.index, pd.Series(1, index=x.index))
        mean_rr = relative_risk.mean()
        paf = (mean_rr - 1) / mean_rr

        return paf

    def get_configuration_defaults(self) -> Dict[str, Dict]:
        return {
            "stratification": {
                self.risk: PAFObserver.configuration_defaults["stratification"][
                    "paf"
                ]
            }
        }
