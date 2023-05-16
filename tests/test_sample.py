# Sample Test passing with nose and pytest
from vivarium import InteractiveContext

from vivarium_nih_us_cvd.constants import paths


def test_pass():
    assert True, "dummy sample test"


def test():
    sim = InteractiveContext(paths.MODEL_SPEC_DIR / "nih_us_cvd.yaml", setup=False)
    sim.configuration.input_data.artifact_path = "/home/rmudambi/scratch/alabama.hdf"
    sim.setup()
    sim.step()
    sim.step()
