from typing import Dict

import pandas as pd
from vivarium_public_health.risks.base_risk import Risk as Risk_

from vivarium_nih_us_cvd.constants.data_values import RISK_EXPOSURE_LIMITS
from vivarium_public_health.risks.data_transformations import get_exposure_post_processor


class Risk(Risk_):
    """Use the standard vivarium_public_health Risk class for risk
    exposure except apply limits to lower and upper bounds (when defined).
    """

    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        exposures = super()._get_current_exposure(index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["minimum"]
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["maximum"]
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures

class SBPRisk(Risk_):
    
    def __init__(self, risk):
        super().__init__(risk)

    def setup(self, builder: 'Builder'):
        # propensity_col = f'{self.risk.name}_propensity'
        # self.population_view = builder.population.get_view([propensity_col])
        # self.propensity = builder.value.register_value_producer(
        #     f'{self.risk.name}.propensity',
        #     source=lambda index: self.population_view.get(index)[propensity_col],
        #     requires_columns=[propensity_col])
        # Need a separate hook to avoid a cyclic dependency at initialization.
        self.base_exposure = builder.value.register_value_producer(
            f'{self.risk.name}.base_exposure',
            source=self.get_current_exposure,
            requires_columns=['age', 'sex'],
            requires_values=[f'{self.risk.name}.propensity'],
            preferred_post_processor=get_exposure_post_processor(builder, self.risk)
        )
        self.exposure = builder.value.register_value_producer(f'{self.risk.name}.exposure', source=self.base_exposure)

    def get_current_exposure(self, index: pd.Index) -> pd.Series:
        breakpoint()
        exposures = super()._get_current_exposure(index)
        if self.risk.name in RISK_EXPOSURE_LIMITS:
            min_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["minimum"]
            max_exposure = RISK_EXPOSURE_LIMITS[self.risk.name]["maximum"]
            exposures[exposures < min_exposure] = min_exposure
            exposures[exposures > max_exposure] = max_exposure
        return exposures