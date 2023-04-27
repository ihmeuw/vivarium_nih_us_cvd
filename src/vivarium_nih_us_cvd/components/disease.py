import pandas as pd
from vivarium.framework.engine import Builder
from vivarium_public_health.disease import (
    DiseaseModel,
    DiseaseState,
    SusceptibleState,
    TransientDiseaseState,
)

from vivarium_nih_us_cvd.constants import data_keys, models
from vivarium_nih_us_cvd.constants.metadata import ARTIFACT_INDEX_COLUMNS


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
    susceptible = SusceptibleState(models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME)
    # states without heart failure
    acute_myocardial_infarction = DiseaseState(
        models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME,
        cause_type="cause",
        get_data_functions={"dwell_time": lambda *args: pd.Timedelta(days=28)},
    )
    post_myocardial_infarction = DiseaseState(
        models.POST_MYOCARDIAL_INFARCTION_STATE_NAME,
        cause_type="cause",
    )
    # states with heart failure
    heart_failure_emr_data_funcs = {
        "excess_mortality_rate": lambda _, builder: builder.data.load(
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
            "disability_weight": lambda _, builder: builder.data.load(
                data_keys.IHD_AND_HF.DISABILITY_WEIGHT_ACUTE_MI
            ),
            "excess_mortality_rate": lambda _, builder: builder.data.load(
                data_keys.IHD_AND_HF.EMR_ACUTE_MI
            ),
        },
    )
    # transient states
    transient_susceptible_state = TransientDiseaseState(
        "transient_susceptible_state",
    )
    transient_post_mi_state = TransientDiseaseState(
        "transient_post_mi_state",
    )

    # transitions out of suspectible state
    susceptible.allow_self_transitions()

    def transient_susceptible_incidence_rate(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        hf_residual_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        return (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence).reset_index()

    susceptible.add_transition(
        transient_susceptible_state,
        source_data_type="rate",
        get_data_functions={"incidence_rate": transient_susceptible_incidence_rate},
    )

    def acute_mi_after_susceptible_proportion(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        hf_residual_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        return (
            acute_mi_incidence
            / (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence)
        ).reset_index()

    transient_susceptible_state.add_transition(
        acute_myocardial_infarction,
        source_data_type="proportion",
        get_data_functions={"proportion": acute_mi_after_susceptible_proportion},
    )

    def hf_ihd_after_susceptible_proportion(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        hf_residual_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        return (
            hf_ihd_incidence / (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence)
        ).reset_index()

    transient_susceptible_state.add_transition(
        heart_failure_from_ihd,
        source_data_type="proportion",
        get_data_functions={"proportion": hf_ihd_after_susceptible_proportion},
    )

    def hf_residual_after_susceptible_proportion(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        hf_residual_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        return (
            hf_residual_incidence
            / (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence)
        ).reset_index()

    transient_susceptible_state.add_transition(
        residual_heart_failure,
        source_data_type="proportion",
        get_data_functions={"proportion": hf_residual_after_susceptible_proportion},
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

    def transient_post_mi_transition_rate(builder: Builder, *_) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        return (acute_mi_incidence + hf_ihd_incidence).reset_index()

    post_myocardial_infarction.add_transition(
        transient_post_mi_state,
        source_data_type="rate",
        get_data_functions={"transition_rate": transient_post_mi_transition_rate},
    )

    def acute_mi_after_post_mi_proportion(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        return (acute_mi_incidence / (acute_mi_incidence + hf_ihd_incidence)).reset_index()

    transient_post_mi_state.add_transition(
        acute_myocardial_infarction,
        source_data_type="proportion",
        get_data_functions={"proportion": acute_mi_after_post_mi_proportion},
    )

    def hf_ihd_out_of_post_mi_proportion(_, builder: Builder) -> pd.DataFrame:
        acute_mi_incidence = builder.data.load(
            data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI
        ).set_index(ARTIFACT_INDEX_COLUMNS)
        hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
            ARTIFACT_INDEX_COLUMNS
        )
        return (hf_ihd_incidence / (acute_mi_incidence + hf_ihd_incidence)).reset_index()

    transient_post_mi_state.add_transition(
        heart_failure_from_ihd,
        source_data_type="proportion",
        get_data_functions={"proportion": hf_ihd_out_of_post_mi_proportion},
    )

    return DiseaseModel(
        models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
        states=[
            susceptible,
            transient_susceptible_state,
            transient_post_mi_state,
            acute_myocardial_infarction,
            post_myocardial_infarction,
            heart_failure_from_ihd,
            acute_myocardial_infarction_and_heart_failure,
            residual_heart_failure,
        ],
    )
