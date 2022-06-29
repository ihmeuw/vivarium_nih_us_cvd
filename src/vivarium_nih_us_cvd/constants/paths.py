from pathlib import Path

import vivarium_nih_us_cvd
from vivarium_nih_us_cvd.constants import metadata

BASE_DIR = Path(vivarium_nih_us_cvd.__file__).resolve().parent

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{metadata.PROJECT_NAME}/")
MODEL_SPEC_DIR = BASE_DIR / 'model_specifications'
RESULTS_ROOT = Path(f'/share/costeffectiveness/results/{metadata.PROJECT_NAME}/')
