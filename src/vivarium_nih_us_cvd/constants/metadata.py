from typing import NamedTuple

# TODO [MIC-3230]: template - import pandas
import pandas as pd

####################
# Project metadata #
####################

PROJECT_NAME = 'vivarium_nih_us_cvd'
# TODO [MIC-3230]: template - change project to 'proj_simscience'
CLUSTER_PROJECT = 'proj_cost_effect'
# # TODO use proj_csu if a csu project
# CLUSTER_PROJECT = 'proj_csu'

CLUSTER_QUEUE = 'all.q'
MAKE_ARTIFACT_MEM = '10G'
MAKE_ARTIFACT_CPU = '1'
MAKE_ARTIFACT_RUNTIME = '3:00:00'
MAKE_ARTIFACT_SLEEP = 10

LOCATIONS = [
    'Alabama',
]

ARTIFACT_INDEX_COLUMNS = [
    'sex',
    'age_start',
    'age_end',
    'year_start',
    'year_end',
]

DRAW_COUNT = 1000
ARTIFACT_COLUMNS = pd.Index([f'draw_{i}' for i in range(DRAW_COUNT)])


class __Scenarios(NamedTuple):
    baseline: str = 'baseline'
    # TODO - add scenarios here


SCENARIOS = __Scenarios()
