from datetime import datetime
from typing import NamedTuple

############################
# Disease Model Parameters #
############################

# REMISSION_RATE = 0.1
# MEAN_SOJOURN_TIME = 10


################################
# Healthcare System Parameters #
################################


class __Visits(NamedTuple):
    NONE: str = "none"
    SCREENING: str = "screening"
    SCHEDULED: str = "scheduled"
    EMERGENCY: str = "emergency"
    SCHEDULED_COLUMN_NAME: str = "scheduled_date"
    MISS_SCHEDULED_COLUMN_NAME: str = "miss_scheduled_visit_probability"

    @property
    def name(self):
        return "visits"


VISITS = __Visits()

PROBABILITY_MISS_SCHEDULED_VISIT_MIN = 0.05
PROBABILITY_MISS_SCHEDULED_VISIT_MAX = 0.35


##############################
# Screening Model Parameters #
##############################

# PROBABILITY_ATTENDING_SCREENING_KEY = "probability_attending_screening"
# PROBABILITY_ATTENDING_SCREENING_START_MEAN = 0.25
# PROBABILITY_ATTENDING_SCREENING_START_STDDEV = 0.0025
# PROBABILITY_ATTENDING_SCREENING_END_MEAN = 0.5
# PROBABILITY_ATTENDING_SCREENING_END_STDDEV = 0.005

# FIRST_SCREENING_AGE = 21
# MID_SCREENING_AGE = 30
# LAST_SCREENING_AGE = 65


###################################
# Scale-up Intervention Constants #
###################################
# SCALE_UP_START_DT = datetime(2021, 1, 1)
# SCALE_UP_END_DT = datetime(2030, 1, 1)
# SCREENING_SCALE_UP_GOAL_COVERAGE = 0.50
# SCREENING_SCALE_UP_DIFFERENCE = (
#     SCREENING_SCALE_UP_GOAL_COVERAGE - PROBABILITY_ATTENDING_SCREENING_START_MEAN
# )


###################
# Risk Parameters #
###################

RISK_EXPOSURE_LIMITS = {
    "high_ldl_cholesterol": {
        "minimum": 0,
        "maximum": 10,
    },
    "high_systolic_blood_pressure": {
        "minimum": 50,
        "maximum": 300,
    },
}
