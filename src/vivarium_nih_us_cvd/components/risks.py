from typing import Dict

import pandas as pd
from vivarium_public_health.risks.base_risk import Risk as Risk_

from vivarium_nih_us_cvd.constants.data_values import RISK_EXPOSURE_LIMITS


# FIXME: would be more proper to modify the distribution function itself
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
