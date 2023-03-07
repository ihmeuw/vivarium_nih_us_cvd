"""Loads, standardizes and validates input data for the simulation.

Abstract the extract and transform pieces of the artifact ETL.
The intent here is to provide a uniform interface around this portion
of artifact creation. The value of this interface shows up when more
complicated data needs are part of the project. See the BEP project
for an example.

`BEP <https://github.com/ihmeuw/vivarium_gates_bep/blob/master/src/vivarium_gates_bep/data/loader.py>`_

.. admonition::

   No logging is done here. Logging is done in vivarium inputs itself and forwarded.
"""
from functools import partial
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from gbd_mapping import ModelableEntity, causes, covariates, risk_factors
from gbd_mapping import sequelae as all_sequelae
from gbd_mapping.base_template import Tmred
from gbd_mapping.id import scalar
from vivarium.framework.artifact import EntityKey
from vivarium_gbd_access import gbd
from vivarium_gbd_access.constants import ROUND_IDS, SEX, SOURCES
from vivarium_gbd_access.utilities import get_draws
from vivarium_inputs import globals as vi_globals
from vivarium_inputs import interface
from vivarium_inputs import utilities as vi_utils
from vivarium_inputs import utility_data
from vivarium_inputs.mapping_extension import (
    alternative_risk_factors,
    healthcare_entities,
)

from vivarium_nih_us_cvd.constants import data_keys, data_values, paths
from vivarium_nih_us_cvd.constants.metadata import (
    ARTIFACT_COLUMNS,
    DRAW_COUNT,
    PROPORTION_DATA_INDEX_COLUMNS,
)
from vivarium_nih_us_cvd.utilities import get_random_variable_draws


def _get_source_key(val: Union[str, data_keys.SourceTarget]) -> str:
    """Retrieves target key for a non-standard output"""
    return val.source if isinstance(val, data_keys.SourceTarget) else val


def get_data(lookup_key: Union[str, data_keys.SourceTarget], location: str) -> pd.DataFrame:
    """Retrieves data from an appropriate source.

    Parameters
    ----------
    lookup_key
        The key that will eventually get put in the artifact with
        the requested data.
    location
        The location to get data for.

    Returns
    -------
        The requested data.

    """
    mapping = {
        # Population
        data_keys.POPULATION.LOCATION: load_population_location,
        data_keys.POPULATION.STRUCTURE: load_population_structure,
        data_keys.POPULATION.AGE_BINS: load_age_bins,
        data_keys.POPULATION.DEMOGRAPHY: load_demographic_dimensions,
        data_keys.POPULATION.TMRLE: load_theoretical_minimum_risk_life_expectancy,
        data_keys.POPULATION.ACMR: load_standard_data,
        data_keys.POPULATION.HEALTHCARE_UTILIZATION: load_healthcare_system_utilization_rate,
        # Cause (ischemic stroke)
        data_keys.ISCHEMIC_STROKE.PREVALENCE_ACUTE: load_prevalence_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.PREVALENCE_CHRONIC: load_prevalence_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE_ACUTE: load_standard_data,
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_ACUTE: load_disability_weight_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_CHRONIC: load_disability_weight_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.EMR_ACUTE: load_emr_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.EMR_CHRONIC: load_emr_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.CSMR: load_standard_data,
        data_keys.ISCHEMIC_STROKE.RESTRICTIONS: load_metadata,
        # Cause (ischemic heart disease and heart failure)
        data_keys.IHD_AND_HF.PREVALENCE_ACUTE_MI: load_prevalence_mi,
        data_keys.IHD_AND_HF.PREVALENCE_ACUTE_MI_AND_HF: load_prevalence_mi,
        data_keys.IHD_AND_HF.PREVALENCE_POST_MI: load_prevalence_mi,
        data_keys.IHD_AND_HF.PREVALENCE_HF_IHD: load_prevalence_heart_failure,
        data_keys.IHD_AND_HF.PREVALENCE_HF_RESIDUAL: load_prevalence_heart_failure,
        data_keys.IHD_AND_HF.INCIDENCE_ACUTE_MI: load_incidence_acute_mi,
        data_keys.IHD_AND_HF.INCIDENCE_HF_IHD: load_incidence_hf_ihd,
        data_keys.IHD_AND_HF.INCIDENCE_HF_RESIDUAL: load_incidence_hf_residual,
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_ACUTE_MI: load_disability_weight_ihd,
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_POST_MI: load_disability_weight_ihd,
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_HF_IHD: load_disability_weight_hf_ihd,
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_HF_RESIDUAL: load_disability_weight_hf_residual,
        data_keys.IHD_AND_HF.EMR_ACUTE_MI: load_emr_ihd_and_hf,
        data_keys.IHD_AND_HF.EMR_POST_MI: load_emr_ihd_and_hf,
        data_keys.IHD_AND_HF.EMR_HF: load_emr_ihd_and_hf,
        data_keys.IHD_AND_HF.CSMR: load_csmr_ihd_and_hf,
        data_keys.IHD_AND_HF.RESTRICTIONS: load_metadata,
        # Risk (LDL-cholesterol)
        data_keys.LDL_C.DISTRIBUTION: load_metadata,
        data_keys.LDL_C.EXPOSURE_MEAN: load_standard_data,
        data_keys.LDL_C.EXPOSURE_SD: load_standard_data,
        data_keys.LDL_C.EXPOSURE_WEIGHTS: load_standard_data,
        data_keys.LDL_C.RELATIVE_RISK: load_standard_data,
        data_keys.LDL_C.PAF: load_standard_data,
        data_keys.LDL_C.TMRED: load_metadata,
        data_keys.LDL_C.RELATIVE_RISK_SCALAR: load_metadata,
        data_keys.LDL_C.MEDICATION_EFFECT: load_ldlc_medication_effect,
        # Risk (systolic blood pressure)
        data_keys.SBP.DISTRIBUTION: load_metadata,
        data_keys.SBP.CATEGORICAL_DISTRIBUTION: load_ordered_polytomous_distribution,
        data_keys.SBP.EXPOSURE_MEAN: load_standard_data,
        data_keys.SBP.EXPOSURE_SD: load_standard_data,
        data_keys.SBP.EXPOSURE_WEIGHTS: load_standard_data,
        data_keys.SBP.RELATIVE_RISK: load_standard_data,
        data_keys.SBP.CATEGORICAL_RELATIVE_RISK: load_relative_risk_categorical_sbp,
        data_keys.SBP.PAF: load_standard_data,
        data_keys.SBP.TMRED: load_metadata,
        data_keys.SBP.RELATIVE_RISK_SCALAR: load_metadata,
        # Risk (body mass index)
        data_keys.BMI.DISTRIBUTION: load_metadata,
        data_keys.BMI.EXPOSURE_MEAN: load_standard_data,
        data_keys.BMI.EXPOSURE_SD: load_bmi_standard_deviation,
        data_keys.BMI.EXPOSURE_WEIGHTS: load_standard_data,
        data_keys.BMI.RELATIVE_RISK: load_relative_risk_bmi,
        data_keys.BMI.PAF: partial(load_standard_data_enforce_minimum, 0),
        data_keys.BMI.TMRED: load_metadata,
        data_keys.BMI.RELATIVE_RISK_SCALAR: load_metadata,
        # Risk (fasting plasma glucose)
        data_keys.FPG.DISTRIBUTION: load_metadata,
        data_keys.FPG.EXPOSURE_MEAN: load_standard_data,
        data_keys.FPG.EXPOSURE_SD: load_standard_data,
        data_keys.FPG.EXPOSURE_WEIGHTS: load_standard_data,
        data_keys.FPG.RELATIVE_RISK: load_standard_data,
        data_keys.FPG.PAF: load_standard_data,
        data_keys.FPG.TMRED: load_metadata,
        data_keys.FPG.RELATIVE_RISK_SCALAR: load_metadata,
        # Risk (ldlc medication adherence)
        data_keys.LDLC_MEDICATION_ADHERENCE.DISTRIBUTION: load_medication_adherence_distribution,
        data_keys.LDLC_MEDICATION_ADHERENCE.EXPOSURE: load_medication_adherence_exposure,
        # Risk (sbp medication adherence)
        data_keys.SBP_MEDICATION_ADHERENCE.DISTRIBUTION: load_medication_adherence_distribution,
        data_keys.SBP_MEDICATION_ADHERENCE.EXPOSURE: load_medication_adherence_exposure,
        # Risk (outreach)
        data_keys.OUTREACH.DISTRIBUTION: load_dichotomous_distribution,
        # Risk (polypill)
        data_keys.POLYPILL.DISTRIBUTION: load_dichotomous_distribution,
    }
    source_key = _get_source_key(lookup_key)
    data = mapping[lookup_key](source_key, location)
    data = handle_special_cases(data, source_key, location)
    return data


def load_population_location(key: str, location: str) -> str:
    if key != data_keys.POPULATION.LOCATION:
        raise ValueError(f"Unrecognized key {key}")

    return location


def load_population_structure(key: str, location: str) -> pd.DataFrame:
    return interface.get_population_structure(location)


def load_age_bins(key: str, location: str) -> pd.DataFrame:
    return interface.get_age_bins()


def load_demographic_dimensions(key: str, location: str) -> pd.DataFrame:
    return interface.get_demographic_dimensions(location)


def load_theoretical_minimum_risk_life_expectancy(key: str, location: str) -> pd.DataFrame:
    return interface.get_theoretical_minimum_risk_life_expectancy()


def _get_measure_wrapped(
    entity: ModelableEntity, measure: Union[str, data_keys.TargetString], location: str
) -> pd.DataFrame:
    """
    All calls to get_measure() need to have the location dropped. For the time being,
    simply use this function.
    """
    return interface.get_measure(entity, measure, location).droplevel("location")


def load_standard_data(key: str, location: str) -> pd.DataFrame:
    key = EntityKey(key)
    entity = get_entity(key)
    return _get_measure_wrapped(entity, key.measure, location)


def load_bmi_standard_deviation(key: str, location: str) -> pd.DataFrame:
    key = EntityKey(key)
    entity = get_entity(key)
    bmi_sd = _get_measure_wrapped(entity, key.measure, location)

    def replace_outliers_by_sampling_from_reasonable_values(row: pd.Series) -> pd.Series:
        outlier_values = row[row >= data_values.MAX_BMI_STANDARD_DEVIATION]
        acceptable_values = row[row < data_values.MAX_BMI_STANDARD_DEVIATION]
        # get average of 50 samples
        new_values = [
            np.mean(acceptable_values.sample(50, replace=True)) for _ in outlier_values
        ]
        return row.replace(dict(zip(outlier_values, new_values)))

    return bmi_sd.apply(replace_outliers_by_sampling_from_reasonable_values)


def load_standard_data_enforce_minimum(
    min_value: Union[float, int], key: str, location: str
) -> pd.DataFrame:
    data = load_standard_data(key, location)
    data[data < min_value] = min_value
    return data


def load_metadata(key: str, location: str):
    key = EntityKey(key)
    entity = get_entity(key)
    entity_metadata = entity[key.measure]
    if hasattr(entity_metadata, "to_dict"):
        entity_metadata = entity_metadata.to_dict()
    return entity_metadata


def load_categorical_paf(key: str, location: str) -> pd.DataFrame:
    try:
        risk = {
            # todo add keys as needed
            data_keys.KEYGROUP.PAF: data_keys.KEYGROUP,
        }[key]
    except KeyError:
        raise ValueError(f"Unrecognized key {key}")

    distribution_type = get_data(risk.DISTRIBUTION, location)

    if distribution_type != "dichotomous" and "polytomous" not in distribution_type:
        raise NotImplementedError(
            f"Unrecognized distribution {distribution_type} for {risk.name}. Only dichotomous and "
            f"polytomous are recognized categorical distributions."
        )

    exp = get_data(risk.EXPOSURE, location)
    rr = get_data(risk.RELATIVE_RISK, location)

    # paf = (sum_categories(exp * rr) - 1) / sum_categories(exp * rr)
    sum_exp_x_rr = (
        (exp * rr)
        .groupby(list(set(rr.index.names) - {"parameter"}))
        .sum()
        .reset_index()
        .set_index(rr.index.names[:-1])
    )
    paf = (sum_exp_x_rr - 1) / sum_exp_x_rr
    return paf


def _load_em_from_meid(location, meid, measure):
    location_id = utility_data.get_location_id(location)
    data = gbd.get_modelable_entity_draws(meid, location_id)
    data = data[data.measure_id == vi_globals.MEASURES[measure]]
    data = vi_utils.normalize(data, fill_value=0)
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS)
    data = vi_utils.reshape(data)
    data = vi_utils.scrub_gbd_conventions(data, location)
    data = vi_utils.split_interval(data, interval_column="age", split_column_prefix="age")
    data = vi_utils.split_interval(data, interval_column="year", split_column_prefix="year")
    return vi_utils.sort_hierarchical_data(data).droplevel("location")


def handle_special_cases(
    data: Union[str, pd.DataFrame],
    source_key: Union[str, data_keys.TargetString],
    location: str,
) -> None:
    source_key = EntityKey(source_key)
    data = match_rr_to_cause_name(data, source_key)
    # use_correct_fpg_name(artifact)
    # modify_hd_incidence(artifact, location)
    return data


def get_entity(key: Union[str, EntityKey]):
    # Map of entity types to their gbd mappings.
    type_map = {
        "cause": causes,
        "covariate": covariates,
        "risk_factor": risk_factors,
        "alternative_risk_factor": alternative_risk_factors,
        "healthcare_entity": healthcare_entities,
    }
    key = EntityKey(key)

    if key.name == "high_fasting_plasma_glucose":
        entity = type_map[key.type][key.name]
        entity.distribution = "ensemble"
        entity.tmred = Tmred(
            distribution="uniform",
            min=scalar(4.884880066),
            max=scalar(5.301205158),
            inverted=False,
        )
        entity.relative_risk_scalar = scalar(1)
        return entity

    return type_map[key.type][key.name]


# Project-specific data functions
def _load_and_sum_prevalence_from_sequelae(
    sequelae: List["Sequela"], location: str
) -> pd.DataFrame:
    return sum(_get_measure_wrapped(s, "prevalence", location) for s in sequelae)


def _get_ischemic_stroke_sequelae() -> Tuple[pd.DataFrame, pd.DataFrame]:
    acute_sequelae = [s for s in causes.ischemic_stroke.sequelae if "acute" in s.name]
    chronic_sequelae = [s for s in causes.ischemic_stroke.sequelae if "chronic" in s.name]
    return acute_sequelae, chronic_sequelae


def load_prevalence_ischemic_stroke(key: str, location: str) -> pd.DataFrame:
    acute_sequelae, chronic_sequelae = _get_ischemic_stroke_sequelae()
    map = {
        data_keys.ISCHEMIC_STROKE.PREVALENCE_ACUTE: acute_sequelae,
        data_keys.ISCHEMIC_STROKE.PREVALENCE_CHRONIC: chronic_sequelae,
    }
    sequelae = map[key]
    prevalence = _load_and_sum_prevalence_from_sequelae(sequelae, location)
    return prevalence


def load_emr_ischemic_stroke(key: str, location: str) -> pd.DataFrame:
    map = {
        data_keys.ISCHEMIC_STROKE.EMR_ACUTE: 24714,
        data_keys.ISCHEMIC_STROKE.EMR_CHRONIC: 10837,
    }
    return _load_em_from_meid(location, map[key], "Excess mortality rate")


def _get_prevalence_weighted_disability_weight(
    seq: List["Sequela"], location: str
) -> List[pd.DataFrame]:
    assert len(seq), "Empty List - get_prevalence_weighted_disability_weight()"
    prevalence_disability_weights = []
    for s in seq:
        prevalence = _get_measure_wrapped(s, "prevalence", location)
        disability_weight = _get_measure_wrapped(s, "disability_weight", location)
        prevalence_disability_weights.append(prevalence * disability_weight)
    return prevalence_disability_weights


def load_disability_weight_ischemic_stroke(key: str, location: str) -> pd.DataFrame:
    acute_sequelae, chronic_sequelae = _get_ischemic_stroke_sequelae()
    map = {
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_ACUTE: acute_sequelae,
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_CHRONIC: chronic_sequelae,
    }
    prevalence_disability_weights = _get_prevalence_weighted_disability_weight(
        map[key], location
    )
    ischemic_stroke_prevalence = _get_measure_wrapped(
        causes.ischemic_stroke, "prevalence", location
    )
    ischemic_stroke_disability_weight = (
        sum(prevalence_disability_weights) / ischemic_stroke_prevalence
    ).fillna(0)
    return ischemic_stroke_disability_weight


def _get_ihd_sequela() -> Dict[str, List["Sequela"]]:
    seq_by_cause = {
        "acute_mi": [
            s
            for s in causes.ischemic_heart_disease.sequelae
            if "acute_myocardial_infarction" in s.name
        ],
        "post_mi": [
            s
            for s in causes.ischemic_heart_disease.sequelae
            if s.name == "asymptomatic_ischemic_heart_disease_following_myocardial_infarction"
        ],
        "heart_failure": [
            s for s in causes.ischemic_heart_disease.sequelae if "heart_failure" in s.name
        ],
    }
    return seq_by_cause


def get_heart_failure_proportions(location: str, heart_failure_type: str) -> pd.Series:
    hf_proportions = pd.read_csv(paths.FILEPATHS.HEART_FAILURE_PROPORTIONS)
    hf_proportions = hf_proportions.query(
        "location_name==@location & sim_cause==@heart_failure_type"
    )

    hf_proportions = hf_proportions[PROPORTION_DATA_INDEX_COLUMNS + ["proportion"]]

    return hf_proportions


def get_proportion_adjusted_heart_failure_data(
    location: str, heart_failure_type: str, measure: str
) -> pd.DataFrame:
    # pull measure data
    location_id = utility_data.get_location_id(location)
    heart_failure_data = gbd.get_modelable_entity_draws(
        data_values.HEART_FAILURE_ME_ID, location_id
    )
    measure_data = heart_failure_data[
        heart_failure_data.measure_id == vi_globals.MEASURES[measure]
    ]
    measure_data = vi_utils.normalize(measure_data, fill_value=0)
    measure_data = measure_data.filter(
        vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS
    )

    # pull proportions data
    hf_proportions = get_heart_failure_proportions(location, heart_failure_type)

    # apply proportion data
    draw_cols = [f"draw_{i}" for i in range(1000)]

    measure_data = measure_data.merge(hf_proportions, on=PROPORTION_DATA_INDEX_COLUMNS)
    measure_data[draw_cols] = measure_data[draw_cols].mul(measure_data["proportion"], axis=0)

    # format for vivarium
    prop_adjusted_data = measure_data.filter(
        vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS
    )
    prop_adjusted_data = vi_utils.reshape(prop_adjusted_data)
    prop_adjusted_data = vi_utils.scrub_gbd_conventions(prop_adjusted_data, location)
    prop_adjusted_data = vi_utils.split_interval(
        prop_adjusted_data, interval_column="age", split_column_prefix="age"
    )
    prop_adjusted_data = vi_utils.split_interval(
        prop_adjusted_data, interval_column="year", split_column_prefix="year"
    )
    prop_adjusted_data = vi_utils.sort_hierarchical_data(prop_adjusted_data).droplevel(
        "location"
    )

    return prop_adjusted_data


def load_prevalence_mi(key: str, location: str) -> pd.DataFrame:
    ihd_seq = _get_ihd_sequela()
    map = {
        data_keys.IHD_AND_HF.PREVALENCE_ACUTE_MI: (ihd_seq["acute_mi"], False),
        data_keys.IHD_AND_HF.PREVALENCE_ACUTE_MI_AND_HF: (ihd_seq["acute_mi"], True),
        data_keys.IHD_AND_HF.PREVALENCE_POST_MI: (ihd_seq["post_mi"], False),
    }
    sequelae, prevalence_includes_heart_failure = map[key]
    sequelae_prevalence = _load_and_sum_prevalence_from_sequelae(sequelae, location)

    hf_ihd_prevalence = load_prevalence_heart_failure(
        data_keys.IHD_AND_HF.PREVALENCE_HF_IHD, location
    )

    if prevalence_includes_heart_failure:
        prevalence = hf_ihd_prevalence * sequelae_prevalence
    else:
        prevalence = (1 - hf_ihd_prevalence) * sequelae_prevalence

    return prevalence


def load_prevalence_heart_failure(key: str, location: str) -> pd.DataFrame:
    heart_failure_type_map = {
        data_keys.IHD_AND_HF.PREVALENCE_HF_IHD: "ihd",
        data_keys.IHD_AND_HF.PREVALENCE_HF_RESIDUAL: "residual",
    }

    heart_failure_type = heart_failure_type_map[key]
    hf_adjusted_prevalence = get_proportion_adjusted_heart_failure_data(
        location, heart_failure_type, "Prevalence"
    )

    return hf_adjusted_prevalence


def load_incidence_acute_mi(key: str, location: str) -> pd.DataFrame:
    # get population mi incidence
    acute_mi_incidence = _load_em_from_meid(
        location, data_values.ACUTE_MI_ME_ID, "Incidence rate"
    )

    # pull prevalences
    acute_mi_sequelae = _get_ihd_sequela()["acute_mi"]
    ami_sequelae_prevalence = _load_and_sum_prevalence_from_sequelae(
        acute_mi_sequelae, location
    )

    hf_residual_prevalence = get_proportion_adjusted_heart_failure_data(
        location, "residual", "Prevalence"
    )

    # calculate prevalence-adjusted incidence
    acute_mi_incidence = acute_mi_incidence / (
        1 - (ami_sequelae_prevalence + hf_residual_prevalence)
    )

    return acute_mi_incidence


def load_incidence_hf_ihd(key: str, location: str) -> pd.DataFrame:
    # pull population incidence data
    hf_ihd_incidence = get_proportion_adjusted_heart_failure_data(
        location, "ihd", "Incidence rate"
    )

    # pull prevalences
    hf_prevalence = _load_em_from_meid(
        location, data_values.HEART_FAILURE_ME_ID, "Prevalence"
    )
    ami_prevalence = load_prevalence_mi(data_keys.IHD_AND_HF.PREVALENCE_ACUTE_MI, location)

    hf_ihd_incidence = hf_ihd_incidence / (1 - (hf_prevalence + ami_prevalence))

    return hf_ihd_incidence


def load_incidence_hf_residual(key: str, location: str) -> pd.DataFrame:
    # pull population incidence data
    hf_residual_incidence = get_proportion_adjusted_heart_failure_data(
        location, "residual", "Incidence rate"
    )

    # pull prevalences
    acute_mi_sequelae = _get_ihd_sequela()["acute_mi"]
    post_mi_sequelae = _get_ihd_sequela()["post_mi"]

    ami_sequelae_prevalence = _load_and_sum_prevalence_from_sequelae(
        acute_mi_sequelae, location
    )
    post_mi_sequelae_prevalence = _load_and_sum_prevalence_from_sequelae(
        post_mi_sequelae, location
    )
    hf_ihd_prevalence = get_proportion_adjusted_heart_failure_data(
        location, "ihd", "Prevalence"
    )
    hf_prevalence = _load_em_from_meid(
        location, data_values.HEART_FAILURE_ME_ID, "Prevalence"
    )

    susceptible_prevalence = 1 - (
        ami_sequelae_prevalence
        + hf_prevalence
        + ((1 - hf_ihd_prevalence) * post_mi_sequelae_prevalence)
    )

    hf_residual_incidence = hf_residual_incidence / susceptible_prevalence

    return hf_residual_incidence


def load_disability_weight_ihd(key: str, location: str) -> pd.DataFrame:
    # get sequelae
    ihd_seq = _get_ihd_sequela()
    map = {
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_ACUTE_MI: ihd_seq["acute_mi"],
        data_keys.IHD_AND_HF.DISABILITY_WEIGHT_POST_MI: ihd_seq["post_mi"],
    }
    sequelae = map[key]

    # calculate disability weights
    prevalence_disability_weights = _get_prevalence_weighted_disability_weight(
        sequelae, location
    )
    prevalence = _load_and_sum_prevalence_from_sequelae(sequelae, location)
    # TODO: Is always filling NA w/ 0 the correct thing here?
    ihd_disability_weight = (sum(prevalence_disability_weights) / prevalence).fillna(0)
    return ihd_disability_weight


def load_disability_weight_hf_ihd(key: str, location: str) -> pd.DataFrame:
    # get sequelae
    sequelae = _get_ihd_sequela()["heart_failure"]

    # calculate disability weights
    prevalence_disability_weights = _get_prevalence_weighted_disability_weight(
        sequelae, location
    )
    prevalence = get_proportion_adjusted_heart_failure_data(location, "ihd", "Prevalence")
    # TODO: Is always filling NA w/ 0 the correct thing here?
    ihd_disability_weight = (sum(prevalence_disability_weights) / prevalence).fillna(0)
    return ihd_disability_weight


def load_disability_weight_hf_residual(key: str, location: str) -> pd.DataFrame:
    # get sequelae
    heart_failure_severities = ["mild", "moderate", "severe", "controlled_medically_managed"]
    sequelae_matching_strings = tuple(
        f"{severity}_heart_failure_due_to" for severity in heart_failure_severities
    )
    sequelae = [
        sequela
        for sequela in all_sequelae
        if (sequela.name.startswith(sequelae_matching_strings))
        & ~("ischemic_heart_disease" in sequela.name)
        & ~("other_cardiovascular_disease" in sequela.name)
        & ~(sequela.name.endswith("due_to_pulmonary_arterial_hypertension"))
    ]

    # calculate disability weights
    prevalence_disability_weights = _get_prevalence_weighted_disability_weight(
        sequelae, location
    )
    prevalence = get_proportion_adjusted_heart_failure_data(
        location, "residual", "Prevalence"
    )
    # TODO: Is always filling NA w/ 0 the correct thing here?
    ihd_disability_weight = (sum(prevalence_disability_weights) / prevalence).fillna(0)
    return ihd_disability_weight


def load_emr_ihd_and_hf(key: str, location: str) -> pd.DataFrame:
    me_id_map = {
        data_keys.IHD_AND_HF.EMR_ACUTE_MI: 24694,
        data_keys.IHD_AND_HF.EMR_POST_MI: 15755,
        data_keys.IHD_AND_HF.EMR_HF: 2412,
    }
    me_id = me_id_map[key]
    return _load_em_from_meid(location, me_id, "Excess mortality rate")


def load_csmr_ihd_and_hf(key: str, location: str) -> pd.DataFrame:
    hf_prevalence = _load_em_from_meid(
        location, data_values.HEART_FAILURE_ME_ID, "Prevalence"
    )
    hf_emr = _load_em_from_meid(
        location, data_values.HEART_FAILURE_ME_ID, "Excess mortality rate"
    )

    acute_sequelae = _get_ihd_sequela()["acute_mi"]
    acute_prevalence = _load_and_sum_prevalence_from_sequelae(acute_sequelae, location)
    acute_emr = _load_em_from_meid(
        location, data_values.ACUTE_MI_ME_ID, "Excess mortality rate"
    )

    post_sequelae = _get_ihd_sequela()["post_mi"]
    post_prevalence = _load_and_sum_prevalence_from_sequelae(post_sequelae, location)
    post_emr = _load_em_from_meid(
        location, data_values.POST_MI_ME_ID, "Excess mortality rate"
    )

    csmr = (
        (hf_prevalence * hf_emr)
        + (acute_prevalence * acute_emr)
        + (post_prevalence * post_emr)
    )

    return csmr


def modify_rr_affected_entity(data: pd.DataFrame, mod_map: Dict[str, List[str]]) -> None:
    """Modify relative_risk data so that the affected_entity and affected_measure
    columns correspond to what is used in the disease model
    """

    def is_transition_rate(name: str) -> bool:
        """affected_measure needs to change to "transition_rate" in some cases"""
        return "_to_" in name

    idx_orig = list(data.index.names)
    data = data.reset_index()
    new_data = []
    for key in mod_map.keys():
        tmp = data.copy()
        tmp = tmp[tmp["affected_entity"] == key]
        for name in mod_map[key]:
            df_new = tmp.copy()
            df_new["affected_entity"] = name
            if is_transition_rate(name):
                df_new["affected_measure"] = "transition_rate"
            new_data += [df_new]
    new_data = pd.concat(new_data, ignore_index=True)
    return new_data.set_index(idx_orig)


def match_rr_to_cause_name(data: Union[str, pd.DataFrame], source_key: EntityKey):
    # Need to make relative risk data match causes in the model
    map = {
        "ischemic_heart_disease": [
            "acute_myocardial_infarction",
            "post_myocardial_infarction_to_acute_myocardial_infarction",
        ],
        "ischemic_stroke": [
            "acute_ischemic_stroke",
            "chronic_ischemic_stroke_to_acute_ischemic_stroke",
        ],
        "heart_failure": [
            "heart_failure_from_ischemic_heart_disease",
            "heart_failure_residual",
        ],
    }
    if source_key.measure in ["relative_risk", "population_attributable_fraction"]:
        data = modify_rr_affected_entity(data, map)
    return data


def load_healthcare_system_utilization_rate(key: str, location: str) -> pd.DataFrame:
    location_id = utility_data.get_location_id(location)
    key = EntityKey(key)
    entity = get_entity(key)
    # vivarium_inputs.core.get_utilization_rate() breaks with the hard-coded
    # gbd_round_id=6; use gbd_round_id=5.
    # TODO: SDB fix in vivarium_gbd_access.gbd.get_modelable_entity_draws()?
    data = get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=entity.gbd_id,
        source=SOURCES.EPI,
        location_id=location_id,
        sex_id=SEX.MALE + SEX.FEMALE,
        age_group_id=gbd.get_age_group_id(),
        gbd_round_id=ROUND_IDS.GBD_2017,
        status="best",
    )
    # Fill in year gaps manually. vi_utils.normalize does not quite work because
    # the data is missing required age_bin edges 2015 and 2019. Instead, let's
    # assume 2018 and 2019 is the same as 2017 and interpolate everything else
    tmp = data[data["year_id"] == 2017]
    for year in [2018, 2019]:
        tmp["year_id"] = year
        data = pd.concat([data, tmp], axis=0)
    data = vi_utils.interpolate_year(data)

    # Cleanup
    data = vi_utils.normalize(data, fill_value=0)
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS)
    data = vi_utils.reshape(data)
    data = vi_utils.scrub_gbd_conventions(data, location)
    data = vi_utils.split_interval(data, interval_column="age", split_column_prefix="age")
    data = vi_utils.split_interval(data, interval_column="year", split_column_prefix="year")
    return vi_utils.sort_hierarchical_data(data).droplevel("location")


def load_ldlc_medication_effect(key: str, location: str) -> pd.DataFrame:
    draws = [f"draw_{i}" for i in range(1000)]
    index = pd.Index(
        [l.DESCRIPTION for l in data_values.LDLC_MEDICATION_EFFICACY],
        name=data_values.COLUMNS.LDLC_MEDICATION,
    )
    df = pd.DataFrame(index=index, columns=draws, dtype=np.float)
    for med_level, seeded_distribution in data_values.LDLC_MEDICATION_EFFICACY:
        df.loc[med_level, :] = get_random_variable_draws(1000, seeded_distribution)
    assert df.notna().values.all()
    return df


def load_relative_risk_categorical_sbp(key: str, location: str) -> pd.DataFrame:
    distributions = data_values.RELATIVE_RISK_SBP_ON_HEART_FAILURE_DISTRIBUTIONS
    population_structure = load_population_structure(
        data_keys.POPULATION.STRUCTURE, location
    ).droplevel("location")

    # define TMREL data
    baseline_hf_rrs = pd.DataFrame(
        1.0, index=population_structure.index, columns=ARTIFACT_COLUMNS
    )
    baseline_hf_rrs["parameter"] = "cat4"

    # define exposed groups data
    exposed_groups_rrs = []
    for sbp_category, distribution in distributions:
        rr_data = get_random_variable_draws(DRAW_COUNT, (sbp_category, distribution))
        # relative risks of 1 for ages without heart failure (under 15)
        under_15_data = pd.DataFrame(
            data=1,
            index=population_structure.query("age_start<15").index,
            columns=ARTIFACT_COLUMNS,
        )
        over_and_including_15_data = pd.DataFrame(
            data=np.repeat(
                [rr_data], len(population_structure.query("age_start>=15")), axis=0
            ),
            index=population_structure.query("age_start>=15").index,
            columns=ARTIFACT_COLUMNS,
        )

        relative_risk_heart_failure = pd.concat([under_15_data, over_and_including_15_data])
        relative_risk_heart_failure["parameter"] = sbp_category

        exposed_groups_rrs.append(relative_risk_heart_failure)

    exposed_rrs = pd.concat(exposed_groups_rrs)

    # define all heart failure data
    heart_failure_rrs = pd.concat([baseline_hf_rrs, exposed_rrs])
    heart_failure_rrs["affected_entity"] = "heart_failure"
    heart_failure_rrs["affected_measure"] = "incidence_rate"
    heart_failure_rrs = heart_failure_rrs.set_index(
        ["affected_entity", "affected_measure", "parameter"], append=True
    )
    heart_failure_rrs = heart_failure_rrs.sort_index()

    return heart_failure_rrs


def load_relative_risk_bmi(key: str, location: str) -> pd.DataFrame:
    standard_rr_data = load_standard_data_enforce_minimum(1, key, location)

    # generate draws for BMI relative risk on heart failure
    rr_data = get_random_variable_draws(
        DRAW_COUNT, data_values.RELATIVE_RISK_BMI_ON_HEART_FAILURE_DISTRIBUTION
    )

    # pull population structure to use as index
    population_structure = load_population_structure(
        data_keys.POPULATION.STRUCTURE, location
    ).droplevel("location")

    # define heart failure rr dataframe
    # relative risks of 1 for ages without heart failure (under 15)
    under_15_data = pd.DataFrame(
        data=1,
        index=population_structure.query("age_start<15").index,
        columns=ARTIFACT_COLUMNS,
    )
    over_and_including_15_data = pd.DataFrame(
        data=np.repeat([rr_data], len(population_structure.query("age_start>=15")), axis=0),
        index=population_structure.query("age_start>=15").index,
        columns=ARTIFACT_COLUMNS,
    )
    relative_risk_heart_failure = pd.concat([under_15_data, over_and_including_15_data])
    relative_risk_heart_failure["affected_entity"] = "heart_failure"
    relative_risk_heart_failure["affected_measure"] = "incidence_rate"
    relative_risk_heart_failure["parameter"] = "per unit"
    relative_risk_heart_failure = relative_risk_heart_failure.set_index(
        ["affected_entity", "affected_measure", "parameter"], append=True
    )

    relative_risk_bmi = pd.concat([standard_rr_data, relative_risk_heart_failure])
    relative_risk_bmi = relative_risk_bmi.sort_index()

    return relative_risk_bmi


def load_medication_adherence_distribution(key: str, location: str) -> str:
    return "ordered_polytomous"


def load_medication_adherence_exposure(key: str, location: str) -> pd.DataFrame:
    df_pop_index = pd.DataFrame(index=load_population_structure(key, location).index)
    # need to add one more level with three options
    df = pd.concat([df_pop_index] * 3)
    df["parameter"] = np.repeat(["cat1", "cat2", "cat3"], len(df_pop_index))
    # Merge on the categorical thresholds
    draws = [f"draw_{i}" for i in range(1000)]
    df = pd.concat([df, pd.DataFrame(columns=draws, dtype=float)])
    # cat1 is most severe -> catN is least severe (tmrel)
    df.loc[
        df["parameter"] == "cat1", draws
    ] = data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY[key.name][
        data_values.MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT
    ]
    df.loc[
        df["parameter"] == "cat2", draws
    ] = data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY[key.name][
        data_values.MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT
    ]
    df.loc[
        df["parameter"] == "cat3", draws
    ] = data_values.MEDICATION_ADHERENCE_TYPE_PROBABILITIY[key.name][
        data_values.MEDICATION_ADHERENCE_TYPE.ADHERENT
    ]
    return df.set_index("parameter", append=True)


def load_dichotomous_distribution(key: str, location: str) -> str:
    return "dichotomous"

def load_ordered_polytomous_distribution(key: str, location: str) -> str:
    return "ordered_polytomous"
