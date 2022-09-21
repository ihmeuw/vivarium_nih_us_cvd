from typing import NamedTuple


##############################
# State table columns to add #
##############################


class __Columns(NamedTuple):
    VISIT_TYPE: str = "visit_type"
    SCHEDULED_VISIT_DATE: str = "scheduled_date"
    MISS_SCHEDULED_VISIT_PROBABILITY: str = "miss_scheduled_visit_probability"
    SBP_MEDICATION: str = "sbp_medication"
    SBP_MEDICATION_ADHERENCE: str = "sbp_medication_adherence"
    LDLC_MEDICATION: str = "ldlc_medication"
    LDLC_MEDICATION_ADHERENCE: str = "ldlc_medication_adherence"

    @property
    def name(self):
        return "columns"


COLUMNS = __Columns()


#####################################
# Healthcare Utilization Parameters #
#####################################

FOLLOWUP_MIN = 3 * 30  # 3 months
FOLLOWUP_MAX = 6 * 30  # 6 months

MISS_SCHEDULED_VISIT_PROBABILITY_MIN = 0.05
MISS_SCHEDULED_VISIT_PROBABILITY_MAX = 0.35


class __VisitType(NamedTuple):
    """visit types to be observed"""

    NONE: str = "none"
    EMERGENCY: str = "emergency"
    SCHEDULED: str = "scheduled"
    MISSED: str = "missed"
    BACKGROUND: str = "background"

    @property
    def name(self):
        return "visit_type"


VISIT_TYPE = __VisitType()


#########################
# Medication Parameters #
#########################


class __SBPMedication(NamedTuple):
    """high sbp medication levels"""

    ONE_DRUG_HALF_DOSE: str = "one_drug_half_dose_efficacy"
    ONE_DRUG_FULL_DOSE: str = "one_drug_std_dose_efficacy"
    TWO_DRUGS_HALF_DOSE: str = "two_drug_half_dose_efficacy"
    TWO_DRUGS_FULL_DOSE: str = "two_drug_std_dose_efficacy"
    THREE_DRUGS_HALF_DOSE: str = "three_drug_half_dose_efficacy"
    THREE_DRUGS_FULL_DOSE: str = "three_drug_std_dose_efficacy"

    @property
    def name(self):
        return "sbp_medication"


SBP_MEDICATION = __SBPMedication()


SBP_MEDICATION_INITIAL_LEVEL_PROBABILITY = {
    SBP_MEDICATION.ONE_DRUG_HALF_DOSE: 0.57,
    SBP_MEDICATION.TWO_DRUGS_HALF_DOSE: 0.43,
}


class __LDLCMedication(NamedTuple):
    """high ldl-c medication levels"""

    LOW: str = "low_intensity"
    MED: str = "medium_intensity"
    LOW_MED_EZE: str = "low_med_with_eze"
    HIGH: str = "high_intensity"
    HIGH_EZE: str = "high_with_eze"

    @property
    def name(self):
        return "ldlc_medication_levels"


LDLC_MEDICATION = __LDLCMedication()


LDLC_MEDICATION_INITIAL_LEVEL_PROBABILITY = {
    LDLC_MEDICATION.LOW: 0.0382,
    LDLC_MEDICATION.MED: 0.7194,
    LDLC_MEDICATION.HIGH: 0.2424,
}


class __MedicationAdherenceType(NamedTuple):

    ADHERENT: str = "adherent"
    PRIMARY_NON_ADHERENT: str = "primary_non_adherent"
    SECONDARY_NON_ADHERENT: str = "secondary_non_adherent"

    @property
    def name(self):
        return "medication_adherence_type"


MEDICATION_ADHERENCE_TYPE = __MedicationAdherenceType()


SBP_MEDICATION_ADHERENCE_VALUE = {
    MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.7392,
    MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.16,
    MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.1008,
}


LDLC_MEDICATION_ADHERENCE_VALUE = {
    MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.6525,
    MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.25,
    MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.0975,
}


class __MedicationAdherenceScore(NamedTuple):
    """adherence score used in medication treatment effect calculation"""

    ADHERENT: float = 1.0
    PRIMARY_NON_ADHERENT: float = 0.0
    SECONDARY_NON_ADHERENT: float = 0.0

    @property
    def name(self):
        return "medication_adherence_score"


MEDICATION_ADHERENCE_SCORE = __MedicationAdherenceScore()


BASELINE_MEDICATION_COVERAGE_SEX_MAPPING = {
    "Female": 2,
    "Male": 1,
}


class __BaselineMedicationCoverageCoefficients(NamedTuple):
    """Coefficients used to calculate baseline medication coverage upon initialization"""

    # Baselie medication coefficients for medication types, like (intercept, sbp, ldlc, age, sex)
    SBP: tuple = (-6.75, 0.025, -0.0045, 0.05, 0.16)
    LDLC: tuple = (-4.23, -0.0026, -0.005, 0.062, -0.19)
    BOTH: tuple = (-6.26, 0.018, -0.014, 0.069, 0.13)

    @property
    def name(self):
        return "baseline_medication_coverage_coefficients"


BASELINE_MEDICATION_COVERAGE_COEFFICIENTS = __BaselineMedicationCoverageCoefficients()


###################
# Risk Parameters #
###################

RISK_EXPOSURE_LIMITS = {
    "high_ldl_cholesterol": {
        "minimum": 0,
        "maximum": 10,
    },
    "high_systolic_blood_pressure": {
        "minimum": 50,
        "maximum": 300,
    },
}
