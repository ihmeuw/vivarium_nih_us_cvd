from vivarium_nih_us_cvd.constants import data_keys


class TransitionString(str):
    def __new__(cls, value):
        # noinspection PyArgumentList
        obj = str.__new__(cls, value.lower())
        obj.from_state, obj.to_state = value.split("_TO_")
        return obj


###########################
# Disease Model variables #
###########################

ISCHEMIC_STROKE_MODEL_NAME = data_keys.ISCHEMIC_STROKE.name
ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME = f"susceptible_to_{ISCHEMIC_STROKE_MODEL_NAME}"
ACUTE_ISCHEMIC_STROKE_STATE_NAME = f"acute_{ISCHEMIC_STROKE_MODEL_NAME}"
CHRONIC_ISCHEMIC_STROKE_STATE_NAME = f"chronic_{ISCHEMIC_STROKE_MODEL_NAME}"
ISCHEMIC_STROKE_MODEL_STATES = (
    ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME,
    ACUTE_ISCHEMIC_STROKE_STATE_NAME,
    CHRONIC_ISCHEMIC_STROKE_STATE_NAME,
)
ISCHEMIC_STROKE_MODEL_TRANSITIONS = (
    TransitionString(
        f"{ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME}_TO_{ACUTE_ISCHEMIC_STROKE_STATE_NAME}"
    ),
    TransitionString(
        f"{ACUTE_ISCHEMIC_STROKE_STATE_NAME}_TO_{CHRONIC_ISCHEMIC_STROKE_STATE_NAME}"
    ),
    TransitionString(
        f"{CHRONIC_ISCHEMIC_STROKE_STATE_NAME}_TO_{ACUTE_ISCHEMIC_STROKE_STATE_NAME}"
    ),
)


MYOCARDIAL_INFARCTION_MODEL_NAME = data_keys.MYOCARDIAL_INFARCTION.name
MYOCARDIAL_INFARCTION_SUSCEPTIBLE_STATE_NAME = (
    f"susceptible_to_{MYOCARDIAL_INFARCTION_MODEL_NAME}"
)
ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME = f"acute_{MYOCARDIAL_INFARCTION_MODEL_NAME}"
POST_MYOCARDIAL_INFARCTION_STATE_NAME = f"post_{MYOCARDIAL_INFARCTION_MODEL_NAME}"
MYOCARDIAL_INFARCTION_MODEL_STATES = (
    MYOCARDIAL_INFARCTION_SUSCEPTIBLE_STATE_NAME,
    ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME,
    POST_MYOCARDIAL_INFARCTION_STATE_NAME,
)
MYOCARDIAL_INFARCTION_MODEL_TRANSITIONS = (
    TransitionString(
        f"{MYOCARDIAL_INFARCTION_SUSCEPTIBLE_STATE_NAME}_TO_{ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME}"
    ),
    TransitionString(
        f"{ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME}_TO_{POST_MYOCARDIAL_INFARCTION_STATE_NAME}"
    ),
    TransitionString(
        f"{POST_MYOCARDIAL_INFARCTION_STATE_NAME}_TO_{ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME}"
    ),
)


# Use the VPH SI("angina") disease model, but define states and transitions
# here as well for post-processing purposes
# Greg requested we remove angina
# ANGINA_MODEL_NAME = data_keys.ANGINA.name
# ANGINA_SUSCEPTIBLE_STATE_NAME = f"susceptible_to_{ANGINA_MODEL_NAME}"
# ANGINA_MODEL_STATES = (
#     ANGINA_SUSCEPTIBLE_STATE_NAME,
#     ANGINA_MODEL_NAME,
# )
# ANGINA_MODEL_TRANSITIONS = (
#     TransitionString(f"{ANGINA_SUSCEPTIBLE_STATE_NAME}_TO_{ANGINA_MODEL_NAME}"),
# )


STATE_MACHINE_MAP = {
    ISCHEMIC_STROKE_MODEL_NAME: {
        "states": ISCHEMIC_STROKE_MODEL_STATES,
        "transitions": ISCHEMIC_STROKE_MODEL_TRANSITIONS,
    },
    MYOCARDIAL_INFARCTION_MODEL_NAME: {
        "states": MYOCARDIAL_INFARCTION_MODEL_STATES,
        "transitions": MYOCARDIAL_INFARCTION_MODEL_TRANSITIONS,
    },
    # Greg requested we remove angina
    # ANGINA_MODEL_NAME: {
    #     "states": ANGINA_MODEL_STATES,
    #     "transitions": ANGINA_MODEL_TRANSITIONS,
    # },
}


STATES = tuple(
    f"{model}_{state}"
    for model, state in STATE_MACHINE_MAP.items()
    for state in STATE_MACHINE_MAP[model]["states"]
)
TRANSITIONS = tuple(
    f"{model}_{state}"
    for model, state in STATE_MACHINE_MAP.items()
    for state in STATE_MACHINE_MAP[model]["transitions"]
)
