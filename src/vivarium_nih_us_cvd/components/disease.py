import pandas as pd
from vivarium_public_health.disease import DiseaseModel, DiseaseState, SusceptibleState

from vivarium_nih_us_cvd.constants import data_keys, models


def IschemicStroke():
    susceptible = SusceptibleState(models.ISCHEMIC_STROKE_MODEL_NAME)
    # SDB - What is 'dwell_time'?
    data_funcs = {'dwell_time': lambda *args: pd.Timedelta(days=28)}
    acute_stroke = DiseaseState(models.ACUTE_ISCHEMIC_STROKE_STATE_NAME, cause_type='sequela', get_data_functions=data_funcs)
    chronic_stroke = DiseaseState(models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME, cause_type='sequela',)

    susceptible.allow_self_transitions()
    data_funcs = {
        'incidence_rate': lambda _, builder: builder.data.load(data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE)
    }
    susceptible.add_transition(acute_stroke, source_data_type='rate', get_data_functions=data_funcs)
    acute_stroke.allow_self_transitions()
    acute_stroke.add_transition(chronic_stroke)
    chronic_stroke.allow_self_transitions()
    data_funcs = {
        'transition_rate': lambda builder, *_: builder.data.load(data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE)
    }
    chronic_stroke.add_transition(acute_stroke, source_data_type='rate', get_data_functions=data_funcs)

    return DiseaseModel(models.ISCHEMIC_STROKE_MODEL_NAME, states=[susceptible, acute_stroke, chronic_stroke])