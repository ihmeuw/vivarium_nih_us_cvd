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
from typing import Dict, List, Tuple, Union

import pandas as pd
from gbd_mapping import causes, covariates, risk_factors
from vivarium.framework.artifact import EntityKey
from vivarium_gbd_access import gbd
from vivarium_inputs import globals as vi_globals
from vivarium_inputs import interface
from vivarium_inputs import utilities as vi_utils
from vivarium_inputs import utility_data
from vivarium_inputs.mapping_extension import alternative_risk_factors

from vivarium_nih_us_cvd.constants import data_keys


def get_data(lookup_key: str, location: str) -> pd.DataFrame:
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
        data_keys.POPULATION.LOCATION: load_population_location,
        data_keys.POPULATION.STRUCTURE: load_population_structure,
        data_keys.POPULATION.AGE_BINS: load_age_bins,
        data_keys.POPULATION.DEMOGRAPHY: load_demographic_dimensions,
        data_keys.POPULATION.TMRLE: load_theoretical_minimum_risk_life_expectancy,
        data_keys.POPULATION.ACMR: load_standard_data,

        data_keys.ISCHEMIC_STROKE.PREVALENCE_ACUTE: load_prevalence_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.PREVALENCE_CHRONIC: load_prevalence_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.INCIDENCE_RATE: load_standard_data,
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_ACUTE: load_disability_weight_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.DISABILITY_WEIGHT_CHRONIC: load_disability_weight_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.EMR_ACUTE: load_emr_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.EMR_CHRONIC: load_emr_ischemic_stroke,
        data_keys.ISCHEMIC_STROKE.CSMR: load_standard_data,
        data_keys.ISCHEMIC_STROKE.RESTRICTIONS: load_metadata,
        
        data_keys.MYOCARDIAL_INFARCTION.PREVALENCE_ACUTE: load_prevalence_ihd,
        data_keys.MYOCARDIAL_INFARCTION.PREVALENCE_POST: load_prevalence_ihd,
        data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_ACUTE: load_incidence_ihd,
        data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_POST: load_incidence_ihd,
        data_keys.MYOCARDIAL_INFARCTION.DISABILITY_WEIGHT_ACUTE: load_disability_weight_ihd,
        data_keys.MYOCARDIAL_INFARCTION.DISABILITY_WEIGHT_POST: load_disability_weight_ihd,
        data_keys.MYOCARDIAL_INFARCTION.EMR_ACUTE: load_emr_ihd,
        data_keys.MYOCARDIAL_INFARCTION.EMR_POST: load_emr_ihd,
        # SDB - Should this go in a higher level data_keys.ISCHEMIC_HEART_DISEASE object
        data_keys.MYOCARDIAL_INFARCTION.CSMR: load_standard_data,
        data_keys.MYOCARDIAL_INFARCTION.RESTRICTIONS: load_metadata,
    }
    return mapping[lookup_key](lookup_key, location)


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
    entity: "ModelableEntity", measure: Union[str, data_keys.TargetString], location: str
) -> pd.DataFrame:
    """
    All calls to get_measure() need to have the location dropped. For the time being,
    simply use this function.
    """
    return interface.get_measure(entity, measure, location).droplevel("location")


def load_standard_data(key: str, location: str) -> pd.DataFrame:
    key = EntityKey(key)
    entity = get_entity(key)
    return interface.get_measure(entity, key.measure, location).droplevel("location")


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


def get_entity(key: Union[str, EntityKey]):
    # Map of entity types to their gbd mappings.
    type_map = {
        "cause": causes,
        "covariate": covariates,
        "risk_factor": risk_factors,
        "alternative_risk_factor": alternative_risk_factors,
    }
    key = EntityKey(key)
    return type_map[key.type][key.name]


# Project-specific data functions
def _load_and_sum_prevalence_from_sequelae(key: str, map: Dict[str, List["Sequela"]], location: str) -> pd.DataFrame:
    return sum(_get_measure_wrapped(s, "prevalence", location) for s in map[key])


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
    prevalence = _load_and_sum_prevalence_from_sequelae(key, map, location)
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
        'acute_mi': [s for s in causes.ischemic_heart_disease.sequelae if "acute_myocardial_infarction" in s.name],
        'post_mi': [s for s in causes.ischemic_heart_disease.sequelae if s.name == "asymptomatic_ischemic_heart_disease_following_myocardial_infarction"],
    }
    return seq_by_cause


def load_prevalence_ihd(key: str, location: str) -> pd.DataFrame:
    ihd_seq = _get_ihd_sequela()
    map = {
        data_keys.MYOCARDIAL_INFARCTION.PREVALENCE_ACUTE: ihd_seq['acute_mi'],
        data_keys.MYOCARDIAL_INFARCTION.PREVALENCE_POST: ihd_seq['post_mi'],
    }
    prevalence = _load_and_sum_prevalence_from_sequelae(key, map, location)
    return prevalence


def load_incidence_ihd(key: str, location: str) -> pd.DataFrame:
    ihd_seq = _get_ihd_sequela()
    map = {
        data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_ACUTE: (ihd_seq['acute_mi'], 24694),
        data_keys.MYOCARDIAL_INFARCTION.INCIDENCE_RATE_POST: (ihd_seq['post_mi'], 24694),
    }
    sequela, meid = map[key]
    incidence = _load_em_from_meid(location, meid, "Incidence rate")
    prevalence = sum(_get_measure_wrapped(s, "prevalence", location) for s in sequela)
    return incidence / (1 - prevalence)


def load_disability_weight_ihd(key: str, location: str) -> pd.DataFrame:
    ihd_seq = _get_ihd_sequela()
    map = {
        data_keys.MYOCARDIAL_INFARCTION.DISABILITY_WEIGHT_ACUTE: ihd_seq['acute_mi'],
        data_keys.MYOCARDIAL_INFARCTION.DISABILITY_WEIGHT_POST: ihd_seq['post_mi'],
    }
    prevalence_disability_weights = _get_prevalence_weighted_disability_weight(map[key], location)
    prevalence = _load_and_sum_prevalence_from_sequelae(key, map, location)
    # TODO: Is always filling NA w/ 0 the correct thing here?
    ihd_disability_weight = (sum(prevalence_disability_weights) / prevalence).fillna(0)
    return ihd_disability_weight


def load_emr_ihd(key: str, location: str) -> pd.DataFrame:
    map = {
        data_keys.MYOCARDIAL_INFARCTION.EMR_ACUTE: 24694,
        data_keys.MYOCARDIAL_INFARCTION.EMR_POST: 15755,
    }
    return _load_em_from_meid(location, map[key], "Excess mortality rate")
