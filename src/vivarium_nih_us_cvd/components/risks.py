from typing import Dict

import pandas as pd
from vivarium_public_health.risks.base_risk import Risk as Risk_

from vivarium_nih_us_cvd.constants.data_values import (
    LDL_C_EXPOSURE_MAXIMUM,
    LDL_C_EXPOSURE_MINIMUM,
)


class LdlcExposure(Risk_):
    """Use the standard vivarium_public_health Risk class for LDL-C
    exposure except limit to lower and upper bounds.
    """
    def _get_current_exposure(self, index: pd.Index) -> pd.Series:
        exposures = super()._get_current_exposure(index)
        exposures[exposures < LDL_C_EXPOSURE_MINIMUM] = LDL_C_EXPOSURE_MINIMUM
        exposures[exposures > LDL_C_EXPOSURE_MAXIMUM] = LDL_C_EXPOSURE_MAXIMUM
        return exposures
