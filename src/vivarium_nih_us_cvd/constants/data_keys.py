from typing import NamedTuple

from vivarium_public_health.utilities import TargetString


#############
# Data Keys #
#############

METADATA_LOCATIONS = "metadata.locations"


class __Population(NamedTuple):
    LOCATION: str = "population.location"
    STRUCTURE: str = "population.structure"
    AGE_BINS: str = "population.age_bins"
    DEMOGRAPHY: str = "population.demographic_dimensions"
    TMRLE: str = "population.theoretical_minimum_risk_life_expectancy"
    ACMR: str = "cause.all_causes.cause_specific_mortality_rate"

    @property
    def name(self):
        return "population"

    @property
    def log_name(self):
        return "population"


POPULATION = __Population()


class __IschemicStroke(NamedTuple):
    PREVALENCE_ACUTE: TargetString = TargetString("sequela.acute_ischemic_stroke.prevalence")
    PREVALENCE_CHRONIC: TargetString = TargetString(
        "sequela.chronic_ischemic_stroke.prevalence"
    )
    INCIDENCE_RATE: TargetString = TargetString("cause.ischemic_stroke.incidence_rate")
    DISABILITY_WEIGHT_ACUTE: TargetString = TargetString(
        "sequela.acute_ischemic_stroke.disability_weight"
    )
    DISABILITY_WEIGHT_CHRONIC: TargetString = TargetString(
        "sequela.chronic_ischemic_stroke.disability_weight"
    )
    EMR_ACUTE: TargetString = TargetString(
        "sequela.acute_ischemic_stroke.excess_mortality_rate"
    )
    EMR_CHRONIC: TargetString = TargetString(
        "sequela.chronic_ischemic_stroke.excess_mortality_rate"
    )
    CSMR: TargetString = TargetString("cause.ischemic_stroke.cause_specific_mortality_rate")
    RESTRICTIONS: TargetString = TargetString("cause.ischemic_stroke.restrictions")

    @property
    def name(self):
        return "ischemic_stroke"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


ISCHEMIC_STROKE = __IschemicStroke()


MAKE_ARTIFACT_KEY_GROUPS = [
    POPULATION,
    ISCHEMIC_STROKE,
]
