from vivarium_nih_us_cvd.constants import data_keys


class TransitionString(str):

    def __new__(cls, value):
        # noinspection PyArgumentList
        obj = str.__new__(cls, value.lower())
        obj.from_state, obj.to_state = value.split('_TO_')
        return obj


###########################
# Disease Model variables #
###########################

ISCHEMIC_STROKE_MODEL_NAME = data_keys.ISCHEMIC_STROKE.name
ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME = f'susceptible_to_{ISCHEMIC_STROKE_MODEL_NAME}'
ACUTE_ISCHEMIC_STROKE_STATE_NAME = 'acute_ischemic_stroke'
CHRONIC_ISCHEMIC_STROKE_STATE_NAME = 'chronic_ischemic_stroke'
ISCHEMIC_STROKE_MODEL_STATES = (
    ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME,
    ACUTE_ISCHEMIC_STROKE_STATE_NAME,
    CHRONIC_ISCHEMIC_STROKE_STATE_NAME,
)
ISCHEMIC_STROKE_MODEL_TRANSITIONS = (
    TransitionString(f'{ISCHEMIC_STROKE_SUSCEPTIBLE_STATE_NAME}_TO_{ACUTE_ISCHEMIC_STROKE_STATE_NAME}'),
    TransitionString(f'{ACUTE_ISCHEMIC_STROKE_STATE_NAME}_TO_{CHRONIC_ISCHEMIC_STROKE_STATE_NAME}'),
    TransitionString(f'{CHRONIC_ISCHEMIC_STROKE_STATE_NAME}_TO_{ACUTE_ISCHEMIC_STROKE_STATE_NAME}'),
)

STATE_MACHINE_MAP = {
    ISCHEMIC_STROKE_MODEL_NAME: {
        'states': ISCHEMIC_STROKE_MODEL_STATES,
        'transitions': ISCHEMIC_STROKE_MODEL_TRANSITIONS,
    },
}


STATES = tuple(state for model in STATE_MACHINE_MAP.values() for state in model['states'])
TRANSITIONS = tuple(state for model in STATE_MACHINE_MAP.values() for state in model['transitions'])
