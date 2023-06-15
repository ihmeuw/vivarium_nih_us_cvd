from typing import NamedTuple

from vivarium_public_health.utilities import TargetString

#############
# Data Keys #
#############

METADATA_LOCATIONS = "metadata.locations"


class SourceTarget(NamedTuple):
    source: str
    target: str


class __Population(NamedTuple):
    LOCATION: str = "population.location"
    STRUCTURE: str = "population.structure"
    AGE_BINS: str = "population.age_bins"
    DEMOGRAPHY: str = "population.demographic_dimensions"
    TMRLE: str = "population.theoretical_minimum_risk_life_expectancy"
    ACMR: str = "cause.all_causes.cause_specific_mortality_rate"
    HEALTHCARE_UTILIZATION: str = "healthcare_entity.outpatient_visits.outpatient_envelope"

    @property
    def name(self):
        return "population"

    @property
    def log_name(self):
        return "population"


POPULATION = __Population()


##########
# Causes #
##########


class __IschemicStroke(NamedTuple):
    PREVALENCE_ACUTE: TargetString = TargetString("sequela.acute_ischemic_stroke.prevalence")
    PREVALENCE_CHRONIC: TargetString = TargetString(
        "sequela.chronic_ischemic_stroke.prevalence"
    )
    INCIDENCE_RATE_ACUTE: TargetString = TargetString("cause.ischemic_stroke.incidence_rate")
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


class __IschemicHeartDiseaseAndHeartFailure(NamedTuple):
    PREVALENCE_ACUTE_MI: TargetString = TargetString(
        "cause.acute_myocardial_infarction.prevalence"
    )
    PREVALENCE_ACUTE_MI_AND_HF: TargetString = TargetString(
        "cause.acute_myocardial_infarction_and_heart_failure.prevalence"
    )
    PREVALENCE_POST_MI: TargetString = TargetString(
        "cause.post_myocardial_infarction.prevalence"
    )
    PREVALENCE_HF_IHD: TargetString = TargetString(
        "cause.heart_failure_from_ischemic_heart_disease.prevalence"
    )
    PREVALENCE_HF_RESIDUAL: TargetString = TargetString(
        "cause.heart_failure_residual.prevalence"
    )
    INCIDENCE_ACUTE_MI: TargetString = TargetString(
        "cause.acute_myocardial_infarction.incidence_rate"
    )
    INCIDENCE_HF_IHD: TargetString = TargetString(
        "cause.heart_failure_from_ischemic_heart_disease.incidence_rate"
    )
    INCIDENCE_HF_RESIDUAL: TargetString = TargetString(
        "cause.heart_failure_residual.incidence_rate"
    )
    DISABILITY_WEIGHT_ACUTE_MI: TargetString = TargetString(
        "cause.acute_myocardial_infarction.disability_weight"
    )
    DISABILITY_WEIGHT_POST_MI: TargetString = TargetString(
        "cause.post_myocardial_infarction.disability_weight"
    )
    DISABILITY_WEIGHT_HF_IHD: TargetString = TargetString(
        "cause.heart_failure_from_ischemic_heart_disease.disability_weight"
    )
    DISABILITY_WEIGHT_HF_RESIDUAL: TargetString = TargetString(
        "cause.heart_failure_residual.disability_weight"
    )
    EMR_ACUTE_MI: TargetString = TargetString(
        "cause.acute_myocardial_infarction.excess_mortality_rate"
    )
    EMR_POST_MI: TargetString = TargetString(
        "cause.post_myocardial_infarction.excess_mortality_rate"
    )
    EMR_HF: TargetString = TargetString("cause.heart_failure.excess_mortality_rate")
    CSMR: TargetString = TargetString(
        "cause.ischemic_heart_disease_and_heart_failure.cause_specific_mortality_rate"
    )
    RESTRICTIONS: SourceTarget = SourceTarget(
        "cause.ischemic_heart_disease.restrictions",
        "cause.ischemic_heart_disease_and_heart_failure.restrictions",
    )

    @property
    def name(self):
        return "ischemic_heart_disease_and_heart_failure"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


IHD_AND_HF = __IschemicHeartDiseaseAndHeartFailure()


################
# Risk Factors #
################


class __HighLDLCholesterol(NamedTuple):
    DISTRIBUTION: TargetString = TargetString("risk_factor.high_ldl_cholesterol.distribution")
    EXPOSURE_MEAN: TargetString = TargetString("risk_factor.high_ldl_cholesterol.exposure")
    EXPOSURE_SD: TargetString = TargetString(
        "risk_factor.high_ldl_cholesterol.exposure_standard_deviation"
    )
    EXPOSURE_WEIGHTS: TargetString = TargetString(
        "risk_factor.high_ldl_cholesterol.exposure_distribution_weights"
    )
    RELATIVE_RISK: TargetString = TargetString(
        "risk_factor.high_ldl_cholesterol.relative_risk"
    )
    # PAF: TargetString = TargetString(
    #     "risk_factor.high_ldl_cholesterol.population_attributable_fraction"
    # )
    TMRED: TargetString = TargetString("risk_factor.high_ldl_cholesterol.tmred")
    RELATIVE_RISK_SCALAR: TargetString = TargetString(
        "risk_factor.high_ldl_cholesterol.relative_risk_scalar"
    )
    MEDICATION_EFFECT: TargetString = TargetString(
        "risk_factor.high_ldl_cholesterol.medication_effect"
    )

    @property
    def name(self):
        return "high_ldl_cholesterol"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


LDL_C = __HighLDLCholesterol()


class __HighSBP(NamedTuple):
    DISTRIBUTION: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.distribution"
    )
    CATEGORICAL_DISTRIBUTION: TargetString = TargetString(
        "risk_factor.categorical_high_systolic_blood_pressure.distribution"
    )
    EXPOSURE_MEAN: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.exposure"
    )
    EXPOSURE_SD: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.exposure_standard_deviation"
    )
    EXPOSURE_WEIGHTS: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.exposure_distribution_weights"
    )
    RELATIVE_RISK: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.relative_risk"
    )
    CATEGORICAL_RELATIVE_RISK: TargetString = TargetString(
        "risk_factor.categorical_high_systolic_blood_pressure.relative_risk"
    )
    # PAF: TargetString = TargetString(
    #     "risk_factor.high_systolic_blood_pressure.population_attributable_fraction"
    # )
    # CATEGORICAL_PAF: TargetString = TargetString(
    #     "risk_factor.categorical_high_systolic_blood_pressure.population_attributable_fraction"
    # )
    TMRED: TargetString = TargetString("risk_factor.high_systolic_blood_pressure.tmred")
    RELATIVE_RISK_SCALAR: TargetString = TargetString(
        "risk_factor.high_systolic_blood_pressure.relative_risk_scalar"
    )

    @property
    def name(self):
        return "high_sbp"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


SBP = __HighSBP()


class __HighBMI(NamedTuple):
    DISTRIBUTION: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.distribution"
    )
    EXPOSURE_MEAN: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.exposure"
    )
    EXPOSURE_SD: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.exposure_standard_deviation"
    )
    EXPOSURE_WEIGHTS: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.exposure_distribution_weights"
    )
    RELATIVE_RISK: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.relative_risk"
    )
    # PAF: TargetString = TargetString(
    #     "risk_factor.high_body_mass_index_in_adults.population_attributable_fraction"
    # )
    TMRED: TargetString = TargetString("risk_factor.high_body_mass_index_in_adults.tmred")
    RELATIVE_RISK_SCALAR: TargetString = TargetString(
        "risk_factor.high_body_mass_index_in_adults.relative_risk_scalar"
    )

    @property
    def name(self):
        return "high_bmi"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


BMI = __HighBMI()


class __HighFPG(NamedTuple):
    DISTRIBUTION: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.distribution"
    )
    EXPOSURE_MEAN: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.exposure"
    )
    EXPOSURE_SD: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.exposure_standard_deviation"
    )
    EXPOSURE_WEIGHTS: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.exposure_distribution_weights"
    )
    RELATIVE_RISK: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.relative_risk"
    )
    # PAF: TargetString = TargetString(
    #     "risk_factor.high_fasting_plasma_glucose.population_attributable_fraction"
    # )
    TMRED: TargetString = TargetString("risk_factor.high_fasting_plasma_glucose.tmred")
    RELATIVE_RISK_SCALAR: TargetString = TargetString(
        "risk_factor.high_fasting_plasma_glucose.relative_risk_scalar"
    )

    @property
    def name(self):
        return "high_fpg"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


FPG = __HighFPG()


class __LDLCholesterolMedicationAdherence(NamedTuple):
    DISTRIBUTION: TargetString = TargetString(
        "risk_factor.ldlc_medication_adherence.distribution"
    )
    EXPOSURE: TargetString = TargetString("risk_factor.ldlc_medication_adherence.exposure")

    @property
    def name(self):
        return "ldlc_medication_adherence"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


LDLC_MEDICATION_ADHERENCE = __LDLCholesterolMedicationAdherence()


class __SBPMedicationAdherence(NamedTuple):
    DISTRIBUTION: TargetString = TargetString(
        "risk_factor.sbp_medication_adherence.distribution"
    )
    EXPOSURE: TargetString = TargetString("risk_factor.sbp_medication_adherence.exposure")

    @property
    def name(self):
        return "sbp_medication_adherence"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


SBP_MEDICATION_ADHERENCE = __SBPMedicationAdherence()


class __Outreach(NamedTuple):
    DISTRIBUTION: TargetString = TargetString("risk_factor.outreach.distribution")

    @property
    def name(self):
        return "outreach"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


OUTREACH = __Outreach()


class __Polypill(NamedTuple):
    DISTRIBUTION: TargetString = TargetString("risk_factor.polypill.distribution")

    @property
    def name(self):
        return "polypill"

    @property
    def log_name(self):
        return self.name.replace("_", " ")


POLYPILL = __Polypill()


##################
# Artifact Items #
##################

MAKE_ARTIFACT_KEY_GROUPS = [
    POPULATION,
    ISCHEMIC_STROKE,
    IHD_AND_HF,
    LDL_C,
    SBP,
    BMI,
    FPG,
    LDLC_MEDICATION_ADHERENCE,
    SBP_MEDICATION_ADHERENCE,
    OUTREACH,
    POLYPILL,
]
