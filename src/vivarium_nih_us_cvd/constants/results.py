import itertools

import pandas as pd

from vivarium_nih_us_cvd.constants import models

#################################
# Results columns and variables #
#################################

TOTAL_POPULATION_COLUMN = "total_population"
TOTAL_YLDS_COLUMN = "years_lived_with_disability"
TOTAL_YLLS_COLUMN = "years_of_life_lost"

# Columns from parallel runs
INPUT_DRAW_COLUMN = "input_draw"
RANDOM_SEED_COLUMN = "random_seed"

OUTPUT_INPUT_DRAW_COLUMN = "input_data.input_draw_number"
OUTPUT_RANDOM_SEED_COLUMN = "randomness.random_seed"
OUTPUT_SCENARIO_COLUMN = "placeholder_branch_name.scenario"

STANDARD_COLUMNS = {
    "total_population": TOTAL_POPULATION_COLUMN,
    "total_ylls": TOTAL_YLLS_COLUMN,
    "total_ylds": TOTAL_YLDS_COLUMN,
}

THROWAWAY_COLUMNS = [f"{state}_event_count" for state in models.STATES]

# FIXME [MIC-3230]: Update to match template. Should we add model_name as prefix to all STATEs and TRANSITIONs?
TOTAL_POPULATION_COLUMN_TEMPLATE = "total_population_{POP_STATE}"
DEATH_COLUMN_TEMPLATE = "death_due_to_{CAUSE_OF_DEATH}_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
YLLS_COLUMN_TEMPLATE = "ylls_due_to_{CAUSE_OF_DEATH}_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
YLDS_COLUMN_TEMPLATE = (
    "ylds_due_to_{CAUSE_OF_DISABILITY}_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
)
STATE_PERSON_TIME_COLUMN_TEMPLATE = (
    "{STATE}_person_time_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
)
TRANSITION_COUNT_COLUMN_TEMPLATE = (
    "{TRANSITION}_event_count_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
)
RISK_EXPOSURE_TIME_TEMPLATE = (
    "total_exposure_time_risk_{RISK}_year_{YEAR}_sex_{SEX}_age_{AGE_GROUP}"
)

COLUMN_TEMPLATES = {
    "population": TOTAL_POPULATION_COLUMN_TEMPLATE,
    "deaths": DEATH_COLUMN_TEMPLATE,
    "ylls": YLLS_COLUMN_TEMPLATE,
    "ylds": YLDS_COLUMN_TEMPLATE,
    "state_person_time": STATE_PERSON_TIME_COLUMN_TEMPLATE,
    "transition_count": TRANSITION_COUNT_COLUMN_TEMPLATE,
    "risk_exposure_time": RISK_EXPOSURE_TIME_TEMPLATE,
}

NON_COUNT_TEMPLATES = []

POP_STATES = ("living", "dead", "tracked", "untracked")
SEXES = ("male", "female")
YEARS = tuple(range(2023, 2041))
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
    "angina",
)
CAUSES_OF_DEATH = CAUSES_OF_DISABILITY + (
    "other_causes",
    models.POST_MYOCARDIAL_INFARCTION_STATE_NAME, # SDB - Post MI has no disability weight
)

RISKS = (
    "high_ldl_cholesterol",
    "high_systolic_blood_pressure",
)

TEMPLATE_FIELD_MAP = {
    "POP_STATE": POP_STATES,
    "YEAR": YEARS,
    "SEX": SEXES,
    "AGE_GROUP": AGE_GROUPS,
    "CAUSE_OF_DEATH": CAUSES_OF_DEATH,
    "CAUSE_OF_DISABILITY": CAUSES_OF_DISABILITY,
    "STATE": models.STATES,
    "TRANSITION": models.TRANSITIONS,
    "RISK": RISKS,
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
