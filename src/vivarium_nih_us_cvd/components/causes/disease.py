import pandas as pd
from vivarium_public_health.disease import DiseaseModel, DiseaseState, SusceptibleState

from vivarium_nih_us_cvd.components.causes.state import (
    MultiTransitionDiseaseState,
    MultiTransitionSusceptibleState,
)
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
        "incidence_rate": lambda builder, *_: builder.data.load(
            data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE_ACUTE
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
            data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE_ACUTE
        )
    }
    chronic_stroke.add_transition(
        acute_stroke, source_data_type="rate", get_data_functions=data_funcs
    )

    return DiseaseModel(
        models.ISCHEMIC_STROKE_MODEL_NAME, states=[susceptible, acute_stroke, chronic_stroke]
    )


def IschemicHeartDiseaseAndHeartFailure():
    susceptible = MultiTransitionSusceptibleState(
        models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME
    )

    # states without heart failure
    acute_myocardial_infarction = DiseaseState(
        models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME,
        cause_type="cause",
        get_data_functions={"dwell_time": lambda *args: pd.Timedelta(days=28)},
    )

    post_myocardial_infarction = MultiTransitionDiseaseState(
        models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
    )
    # states with heart failure
    heart_failure_emr_data_funcs = {
        "excess_mortality_rate": lambda builder, _: builder.data.load(
            data_keys.IHD_AND_HF.EMR_HF
        )
    }
    heart_failure_from_ihd = DiseaseState(
        models.HEART_FAILURE_FROM_ISCHEMIC_HEART_DISEASE_STATE_NAME,
        cause_type="cause",
        get_data_functions=heart_failure_emr_data_funcs,
    )
    residual_heart_failure = DiseaseState(
        models.HEART_FAILURE_RESIDUAL_STATE_NAME,
        cause_type="cause",
        get_data_functions=heart_failure_emr_data_funcs,
    )
    acute_myocardial_infarction_and_heart_failure = DiseaseState(
        models.ACUTE_MYOCARDIAL_INFARCTION_AND_HEART_FAILURE_STATE_NAME,
        cause_type="cause",
        get_data_functions={
            "dwell_time": lambda *args: pd.Timedelta(days=28),
            "disability_weight": lambda builder, _: builder.data.load(
                data_keys.IHD_AND_HF.DISABILITY_WEIGHT_ACUTE_MI
            ),
            "excess_mortality_rate": lambda builder, _: builder.data.load(
                data_keys.IHD_AND_HF.EMR_ACUTE_MI
            ),
        },
    )

    # transitions out of susceptible state
    susceptible.allow_self_transitions()

    susceptible.add_transition(
        acute_myocardial_infarction,
        source_data_type="rate",
        get_data_functions={
            "incidence_rate": lambda builder, cause: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
            )
        },
    )

    susceptible.add_transition(
        heart_failure_from_ihd,
        source_data_type="rate",
        get_data_functions={
            "incidence_rate": lambda builder, cause: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_HF_IHD
            )
        },
    )

    susceptible.add_transition(
        residual_heart_failure,
        source_data_type="rate",
        get_data_functions={
            "incidence_rate": lambda builder, cause: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
            )
        },
    )

    # transitions out of heart failure from IHD state
    heart_failure_from_ihd.allow_self_transitions()
    heart_failure_from_ihd.add_transition(
        acute_myocardial_infarction_and_heart_failure,
        source_data_type="rate",
        get_data_functions={
            "transition_rate": lambda builder, *_: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
            )
        },
    )

    # transitions out of acute MI states
    acute_myocardial_infarction.allow_self_transitions()
    acute_myocardial_infarction.add_transition(post_myocardial_infarction)

    acute_myocardial_infarction_and_heart_failure.allow_self_transitions()
    acute_myocardial_infarction_and_heart_failure.add_transition(heart_failure_from_ihd)

    # transitions out of post MI states
    post_myocardial_infarction.allow_self_transitions()

    post_myocardial_infarction.add_transition(
        acute_myocardial_infarction,
        source_data_type="rate",
        get_data_functions={
            "transition_rate": lambda builder, *_: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
            )
        },
    )

    post_myocardial_infarction.add_transition(
        heart_failure_from_ihd,
        source_data_type="rate",
        get_data_functions={
            "transition_rate": lambda builder, *_: builder.data.load(
                data_keys.IHD_AND_HF.INCIDENCE_HF_IHD
            )
        },
    )

    return DiseaseModel(
        models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
        states=[
            susceptible,
            acute_myocardial_infarction,
            post_myocardial_infarction,
            heart_failure_from_ihd,
            acute_myocardial_infarction_and_heart_failure,
            residual_heart_failure,
        ],
    )