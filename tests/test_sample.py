# Sample Test passing with nose and pytest
from vivarium_nih_us_cvd.components.causes import Causes


def test_pass():
    assert True, "dummy sample test"


def test_causes_config():
    Causes()
