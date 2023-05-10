from pathlib import Path
from typing import NamedTuple

import vivarium_nih_us_cvd
from vivarium_nih_us_cvd.constants import metadata

BASE_DIR = Path(vivarium_nih_us_cvd.__file__).resolve().parent

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{metadata.PROJECT_NAME}/")
MODEL_SPEC_DIR = BASE_DIR / "model_specifications"
RESULTS_ROOT = Path(f"/share/costeffectiveness/results/{metadata.PROJECT_NAME}/")
DATA_ROOT = BASE_DIR / "data"

CAUSE_RISK_CONFIG = BASE_DIR / "configuration" / "causes.yaml"


class __Filepaths(NamedTuple):
    """Specific filepaths container"""

    SBP_MEDICATION_EFFECTS: Path = DATA_ROOT / "drug_efficacy_sbp.csv"
    HEART_FAILURE_PROPORTIONS: Path = DATA_ROOT / "hf_props.csv"
    CALCULATED_PAFS: Path = DATA_ROOT / "calculated_pafs.hdf"
    LDL_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "ldl_weights.csv"
    SBP_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "sbp_weights.csv"
    BMI_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "bmi_weights.csv"
    FPG_STANDARD_DEVIATION: Path = DATA_ROOT / "fpg_standard_deviation.csv"

    @property
    def name(self):
        return "filepaths"


FILEPATHS = __Filepaths()
