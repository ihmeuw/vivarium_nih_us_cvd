import pandas as pd
from vivarium_public_health.disease import DiseaseModel, DiseaseState, SusceptibleState

from vivarium_nih_us_cvd.constants import data_keys, models


def IschemicStroke():
    susceptible = SusceptibleState(models.ISCHEMIC_STROKE_MODEL_NAME)
    data_funcs = {"dwell_time": lambda *args: pd.Timedelta(days=28)}
    acute_stroke = DiseaseState(
        models.ACUTE_ISCHEMIC_STROKE_STATE_NAME,
        cause_type="sequela",
        get_data_functions=data_funcs,
    )
    chronic_stroke = DiseaseState(
        models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME,
        cause_type="sequela",
    )

    susceptible.allow_self_transitions()
    data_funcs = {
        "incidence_rate": lambda _, builder: builder.data.load(
            data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE
        )
    }
    susceptible.add_transition(
        acute_stroke, source_data_type="rate", get_data_functions=data_funcs
    )
    acute_stroke.allow_self_transitions()
    acute_stroke.add_transition(chronic_stroke)
    chronic_stroke.allow_self_transitions()
    data_funcs = {
        "transition_rate": lambda builder, *_: builder.data.load(
            data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE
        )
    }
    chronic_stroke.add_transition(
        acute_stroke, source_data_type="rate", get_data_functions=data_funcs
    )

    return DiseaseModel(
        models.ISCHEMIC_STROKE_MODEL_NAME, states=[susceptible, acute_stroke, chronic_stroke]
    )


def MyocardialInfarction():
    susceptible = SusceptibleState(models.MYOCARDIAL_INFARCTION_MODEL_NAME)
    data_funcs = {"dwell_time": lambda *args: pd.Timedelta(days=28)}
    acute_myocardial_infarction = DiseaseState(
        models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME,
        # SDB - how do I know if the cause type is 'cause' or 'sequela'?
        cause_type="cause",
        get_data_functions=data_funcs,
    )
    post_myocardial_infarction = DiseaseState(
        models.POST_MYOCARDIAL_INFARCTION_STATE_NAME,
        cause_type="cause",
    )

    susceptible.allow_self_transitions()
    data_funcs = {"incidence_rate": lambda _, builder: builder.data.load(data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_ACUTE)}
    susceptible.add_transition(acute_myocardial_infarction, source_data_type="rate", get_data_functions=data_funcs)
    acute_myocardial_infarction.allow_self_transitions()
    acute_myocardial_infarction.add_transition(post_myocardial_infarction)
    post_myocardial_infarction.allow_self_transitions()
    data_funcs = {"transition_rate": lambda builder, *_: builder.data.load(data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_POST)}
    post_myocardial_infarction.add_transition(
        acute_myocardial_infarction, source_data_type="rate", get_data_functions=data_funcs
    )

    return DiseaseModel(
        models.MYOCARDIAL_INFARCTION_MODEL_NAME, states=[susceptible, acute_myocardial_infarction, post_myocardial_infarction]
    )