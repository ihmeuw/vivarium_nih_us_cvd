import itertools

import pandas as pd

from vivarium_nih_us_cvd.constants import data_values, models
from vivarium_nih_us_cvd.constants.models import STATE_MACHINE_MAP

#################################
# Results columns and variables #
#################################

TOTAL_POPULATION_COLUMN = "total_population"
TOTAL_YLDS_COLUMN = "years_lived_with_disability"
TOTAL_YLLS_COLUMN = "years_of_life_lost"

# Columns from parallel runs
INPUT_DRAW_COLUMN = "input_draw"
RANDOM_SEED_COLUMN = "random_seed"
LOCATION_COLUMN = "location"

OUTPUT_INPUT_DRAW_COLUMN = "input_data.input_draw_number"
OUTPUT_RANDOM_SEED_COLUMN = "randomness.random_seed"
OUTPUT_SCENARIO_COLUMN = "intervention.scenario"
OUTPUT_ARTIFACT_COLUMN = "run_configuration.run_key.input_data.artifact_path"

STANDARD_COLUMNS = {
    "total_ylls": TOTAL_YLLS_COLUMN,
    "total_ylds": TOTAL_YLDS_COLUMN,
}

THROWAWAY_COLUMNS = [f"{state}_event_count" for state in models.STATES]

# FIXME [MIC-3230]: Update to match template. Should we add model_name as prefix to all STATEs and TRANSITIONs?
DEATH_COLUMN_TEMPLATE = "MEASURE_death_due_to_{CAUSE_OF_DEATH}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
YLLS_COLUMN_TEMPLATE = "MEASURE_ylls_due_to_{CAUSE_OF_DEATH}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
YLDS_COLUMN_TEMPLATE = (
    "MEASURE_ylds_due_to_{CAUSE_OF_DISABILITY}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
STATE_PERSON_TIME_COLUMN_TEMPLATE = (
    "MEASURE_{STATE}_person_time_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
TRANSITION_COUNT_COLUMN_TEMPLATE = (
    "MEASURE_{TRANSITION}_event_count_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
RISK_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_{RISK}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
RISK_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_{RISK}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_LOW_LDL_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_ldl_cholesterol_below_2.59_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_FIRST_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_ldl_cholesterol_between_2.59_and_3.36_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_SECOND_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_ldl_cholesterol_between_3.36_and_4.14_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_THIRD_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_ldl_cholesterol_between_4.14_and_4.91_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_HIGH_LDL_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_ldl_cholesterol_above_4.91_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_LOW_SBP_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_systolic_blood_pressure_below_130_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_MEDIUM_SBP_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_systolic_blood_pressure_between_130_and_140_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
BINNED_HIGH_SBP_EXPOSURE_TIME_TEMPLATE = (
    "MEASURE_total_exposure_time_risk_high_systolic_blood_pressure_above_140_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)
VISIT_COUNT_TEMPLATE = "MEASURE_healthcare_visits_{VISIT_TYPE}_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
SBP_MEDICATION_PERSON_TIME_TEMPLATE = "MEASURE_sbp_medication_{SBP_MEDICATION}_person_time_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SBP_MEDICATION_ADHERENCE_{MEDICATION_ADHERENCE}_SEX_{SEX}"
LDLC_MEDICATION_PERSON_TIME_TEMPLATE = "MEASURE_ldlc_medication_{LDLC_MEDICATION}_person_time_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_LDLC_MEDICATION_ADHERENCE_{MEDICATION_ADHERENCE}_SEX_{SEX}"
INTERVENTION_PERSON_TIME_TEMPLATE = (
    "MEASURE_{INTERVENTION_TYPE}_{INTERVENTION}_person_time_AGE_GROUP_{AGE_GROUP}_CURRENT_YEAR_{YEAR}_SEX_{SEX}"
)


COLUMN_TEMPLATES = {
    "deaths": DEATH_COLUMN_TEMPLATE,
    "ylls": YLLS_COLUMN_TEMPLATE,
    "ylds": YLDS_COLUMN_TEMPLATE,
    "state_person_time": STATE_PERSON_TIME_COLUMN_TEMPLATE,
    "transition_count": TRANSITION_COUNT_COLUMN_TEMPLATE,
    "risk_exposure_time": RISK_EXPOSURE_TIME_TEMPLATE,
    "binned_low_ldl_risk_exposure_time": BINNED_LOW_LDL_EXPOSURE_TIME_TEMPLATE,
    "binned_first_medium_ldl_risk_exposure_time": BINNED_FIRST_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE,
    "binned_second_medium_ldl_risk_exposure_time": BINNED_SECOND_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE,
    "binned_third_medium_ldl_risk_exposure_time": BINNED_THIRD_MEDIUM_LDL_EXPOSURE_TIME_TEMPLATE,
    "binned_high_ldl_risk_exposure_time": BINNED_HIGH_LDL_EXPOSURE_TIME_TEMPLATE,
    "binned_low_sbp_risk_exposure_time": BINNED_LOW_SBP_EXPOSURE_TIME_TEMPLATE,
    "binned_medium_sbp_risk_exposure_time": BINNED_MEDIUM_SBP_EXPOSURE_TIME_TEMPLATE,
    "binned_high_sbp_risk_exposure_time": BINNED_HIGH_SBP_EXPOSURE_TIME_TEMPLATE,
    "healthcare_visits": VISIT_COUNT_TEMPLATE,
    "sbp_medication_person_time": SBP_MEDICATION_PERSON_TIME_TEMPLATE,
    "ldlc_medication_person_time": LDLC_MEDICATION_PERSON_TIME_TEMPLATE,
    "intervention_person_time": INTERVENTION_PERSON_TIME_TEMPLATE,
}

NON_COUNT_TEMPLATES = []

SEXES = ("Male", "Female")
YEARS = tuple(range(2021, 2041))
AGE_GROUPS = (
    "25_to_29",
    "30_to_34",
    "35_to_39",
    "40_to_44",
    "45_to_49",
    "50_to_54",
    "55_to_59",
    "60_to_64",
    "65_to_69",
    "70_to_74",
    "75_to_79",
    "80_to_84",
    "85_to_89",
    "90_to_94",
    "95_plus",
)
CAUSES_OF_DISABILITY = (
    models.ACUTE_ISCHEMIC_STROKE_STATE_NAME,
    models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME,
    models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME,
    models.HEART_FAILURE_FROM_ISCHEMIC_HEART_DISEASE_STATE_NAME,
    models.ACUTE_MYOCARDIAL_INFARCTION_AND_HEART_FAILURE_STATE_NAME,
    models.HEART_FAILURE_RESIDUAL_STATE_NAME,
)
CAUSES_OF_DEATH = CAUSES_OF_DISABILITY + (
    "other_causes",
    models.POST_MYOCARDIAL_INFARCTION_STATE_NAME,  # SDB - Post MI has no disability weight
)
RISKS = (
    "high_ldl_cholesterol",
    "high_systolic_blood_pressure",
    "high_body_mass_index_in_adults",
    "high_fasting_plasma_glucose",
)
MEDICATION_ADHERENCES = tuple(x for x in data_values.MEDICATION_ADHERENCE_TYPE)
SBP_MEDICATIONS = tuple(x.DESCRIPTION for x in data_values.SBP_MEDICATION_LEVEL)
LDLC_MEDICATIONS = tuple(x.DESCRIPTION for x in data_values.LDLC_MEDICATION_LEVEL)
INTERVENTION_TYPES = (
    "outreach",
    "polypill",
    "lifestyle",
)
INTERVENTIONS = ("cat1", "cat2")

TEMPLATE_FIELD_MAP = {
    "YEAR": YEARS,
    "SEX": SEXES,
    "AGE_GROUP": AGE_GROUPS,
    "CAUSE_OF_DEATH": CAUSES_OF_DEATH,
    "CAUSE_OF_DISABILITY": CAUSES_OF_DISABILITY,
    "STATE": [state for model in STATE_MACHINE_MAP for state in STATE_MACHINE_MAP[model]['states']],
    "TRANSITION": [transition for model in STATE_MACHINE_MAP for transition in STATE_MACHINE_MAP[model]['transitions']],
    "RISK": RISKS,
    "VISIT_TYPE": data_values.VISIT_TYPE,
    "MEDICATION_ADHERENCE": MEDICATION_ADHERENCES,
    "SBP_MEDICATION": SBP_MEDICATIONS,
    "LDLC_MEDICATION": LDLC_MEDICATIONS,
    "INTERVENTION_TYPE": INTERVENTION_TYPES,
    "INTERVENTION": INTERVENTIONS,
}


# noinspection PyPep8Naming
def RESULT_COLUMNS(kind="all"):
    if kind not in COLUMN_TEMPLATES and kind != "all":
        raise ValueError(f"Unknown result column type {kind}")
    columns = []
    if kind == "all":
        for k in COLUMN_TEMPLATES:
            columns += RESULT_COLUMNS(k)
        columns = list(STANDARD_COLUMNS.values()) + columns
    else:
        template = COLUMN_TEMPLATES[kind]
        filtered_field_map = {
            field: values
            for field, values in TEMPLATE_FIELD_MAP.items()
            if f"{{{field}}}" in template
        }
        fields, value_groups = filtered_field_map.keys(), itertools.product(
            *filtered_field_map.values()
        )
        for value_group in value_groups:
            columns.append(
                template.format(**{field: value for field, value in zip(fields, value_group)})
            )
    return columns


# noinspection PyPep8Naming
def RESULTS_MAP(kind):
    if kind not in COLUMN_TEMPLATES:
        raise ValueError(f"Unknown result column type {kind}")
    columns = []
    template = COLUMN_TEMPLATES[kind]
    filtered_field_map = {
        field: values
        for field, values in TEMPLATE_FIELD_MAP.items()
        if f"{{{field}}}" in template
    }
    fields, value_groups = list(filtered_field_map.keys()), list(
        itertools.product(*filtered_field_map.values())
    )
    for value_group in value_groups:
        columns.append(
            template.format(**{field: value for field, value in zip(fields, value_group)})
        )
    df = pd.DataFrame(value_groups, columns=map(lambda x: x.lower(), fields))
    df["key"] = columns
    df[
        "measure"
    ] = kind  # per researcher feedback, this column is useful, even when it's identical for all rows
    return df.set_index("key").sort_index()
