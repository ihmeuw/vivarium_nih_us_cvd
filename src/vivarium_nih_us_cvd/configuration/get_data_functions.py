import pandas as pd
from vivarium.framework.engine import Builder

from vivarium_nih_us_cvd.constants import data_keys
from vivarium_nih_us_cvd.constants.metadata import ARTIFACT_INDEX_COLUMNS


def transient_susceptible_incidence_rate(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_residual_incidence = builder.data.load(
        data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
    ).set_index(ARTIFACT_INDEX_COLUMNS)
    return (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence).reset_index()


def acute_mi_after_susceptible_proportion(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_residual_incidence = builder.data.load(
        data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
    ).set_index(ARTIFACT_INDEX_COLUMNS)
    return (
        acute_mi_incidence / (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence)
    ).reset_index()


def hf_ihd_after_susceptible_proportion(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_residual_incidence = builder.data.load(
        data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL
    ).set_index(ARTIFACT_INDEX_COLUMNS)
    return (
        hf_ihd_incidence / (acute_mi_incidence + hf_ihd_incidence + hf_residual_incidence)
    ).reset_index()


def hf_residual_after_susceptible_proportion(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
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


def transient_post_mi_transition_rate(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    return (acute_mi_incidence + hf_ihd_incidence).reset_index()


def acute_mi_after_post_mi_proportion(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    return (acute_mi_incidence / (acute_mi_incidence + hf_ihd_incidence)).reset_index()


def hf_ihd_out_of_post_mi_proportion(builder: Builder, *_) -> pd.DataFrame:
    acute_mi_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    hf_ihd_incidence = builder.data.load(data_keys.IHD_AND_HF.INCIDENCE_HF_IHD).set_index(
        ARTIFACT_INDEX_COLUMNS
    )
    return (hf_ihd_incidence / (acute_mi_incidence + hf_ihd_incidence)).reset_index()
