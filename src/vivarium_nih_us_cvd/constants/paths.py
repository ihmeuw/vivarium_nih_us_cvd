from pathlib import Path
from typing import NamedTuple

import vivarium_nih_us_cvd
from vivarium_nih_us_cvd.constants import metadata

BASE_DIR = Path(vivarium_nih_us_cvd.__file__).resolve().parent

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{metadata.PROJECT_NAME}/")
MODEL_SPEC_DIR = BASE_DIR / "model_specifications"
RESULTS_ROOT = Path(f"/share/costeffectiveness/results/{metadata.PROJECT_NAME}/")
DATA_ROOT = BASE_DIR / "data"


class __Filepaths(NamedTuple):
    """Specific filepaths container"""

    SBP_MEDICATION_EFFECTS: Path = DATA_ROOT / "drug_efficacy_sbp.csv"
    LDLC_MEDICATION_EFFECTS: Path = DATA_ROOT / "drug_efficacy_ldl.csv"

    @property
    def name(self):
        return "filepaths"


FILEPATHS = __Filepaths()
