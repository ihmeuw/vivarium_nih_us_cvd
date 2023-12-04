from pathlib import Path
from typing import NamedTuple

import vivarium_nih_us_cvd
from vivarium_nih_us_cvd.constants import metadata

BASE_DIR = Path(vivarium_nih_us_cvd.__file__).resolve().parent
BASE_OUTPUT_DIR = Path("/mnt/team/simulation_science/pub/models/")

ARTIFACT_ROOT = BASE_OUTPUT_DIR / metadata.PROJECT_NAME / "artifacts"
DATA_ROOT = BASE_DIR / "data"

CAUSE_RISK_CONFIG = BASE_DIR / "components" / "causes" / "causes.yaml"


class __Filepaths(NamedTuple):
    """Specific filepaths container"""

    SBP_MEDICATION_EFFECTS: Path = DATA_ROOT / "drug_efficacy_sbp.csv"
    HEART_FAILURE_PROPORTIONS: Path = DATA_ROOT / "hf_props.csv"
    LDL_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "ldl_weights.csv"
    SBP_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "sbp_weights.csv"
    BMI_DISTRIBUTION_WEIGHTS: Path = DATA_ROOT / "bmi_weights.csv"
    FPG_STANDARD_DEVIATION: Path = DATA_ROOT / "fpg_standard_deviation.csv"
    RISK_CORRELATION: Path = DATA_ROOT / "risk_correlation.csv"
    MEDIATION_FACTORS: Path = DATA_ROOT / "mediation_factors.csv"
    HEART_FAILURE_MEDIATION_DELTAS: Path = DATA_ROOT / "heart_failure_deltas_all_draws.csv"
    MEDICATION_COVERAGE_RRS: Path = DATA_ROOT / "state_medication_data.csv"

    @property
    def name(self):
        return "filepaths"


FILEPATHS = __Filepaths()
