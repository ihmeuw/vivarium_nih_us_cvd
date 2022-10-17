from typing import NamedTuple

#######################
# State table columns #
#######################


class __Columns(NamedTuple):
    """column names"""

    VISIT_TYPE: str = "visit_type"
    SCHEDULED_VISIT_DATE: str = "scheduled_date"
    SBP_MEDICATION: str = "sbp_medication"
    SBP_MEDICATION_ADHERENCE: str = "sbp_medication_adherence"
    LDLC_MEDICATION: str = "ldlc_medication"
    LDLC_MEDICATION_ADHERENCE: str = "ldlc_medication_adherence"
    BASELINE_SBP_MEDICATION: str = "baseline_sbp_medication"
    BASELINE_LDLC_MEDICATION: str = "baseline_ldlc_medication"

    @property
    def name(self):
        return "columns"


COLUMNS = __Columns()


##################
# Pipeline names #
##################


class __Pipelines(NamedTuple):
    """value pipeline names"""

    SBP_EXPOSURE: str = "high_systolic_blood_pressure.exposure"
    LDLC_EXPOSURE: str = "high_ldl_cholesterol.exposure"

    @property
    def name(self):
        return "pipelines"


PIPELINES = __Pipelines()


########################
# Component priorities #
########################


class __ComponentPriorities(NamedTuple):
    """component listenr priorities that require something other than the default (5)"""

    HEALTHCARE_VISITS: int = 5
    TREATMENT: int = 6

    @property
    def name(self):
        return "component_priorities"


COMPONENT_PRIORITIES = __ComponentPriorities()


#####################################
# Healthcare Utilization Parameters #
#####################################

FOLLOWUP_MIN = 3 * 30  # 3 months
FOLLOWUP_MAX = 6 * 30  # 6 months

MISS_SCHEDULED_VISIT_PROBABILITY = 0.0868

MEASUREMENT_ERROR_MEAN_SBP = 0  # mmHg
MEASUREMENT_ERROR_SD_SBP = 2.9  # mmHg


class __VisitType(NamedTuple):
    """healthcare visit types"""

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

THERAPEUTIC_INERTIA_NO_START = (
    0.4176  # The chance that a patient will not have medication changed
)


class __SBPThreshold(NamedTuple):
    """sbp exposure thresholds"""

    LOW: int = 130
    HIGH: int = 140

    @property
    def name(self):
        return "sbp_threshold"


SBP_THRESHOLD = __SBPThreshold()


class __SBPMedicationLevel(NamedTuple):
    """high sbp medication level"""

    ONE_DRUG_HALF_DOSE: str = "one_drug_half_dose_efficacy"
    ONE_DRUG_FULL_DOSE: str = "one_drug_std_dose_efficacy"
    TWO_DRUGS_HALF_DOSE: str = "two_drug_half_dose_efficacy"
    TWO_DRUGS_FULL_DOSE: str = "two_drug_std_dose_efficacy"
    THREE_DRUGS_HALF_DOSE: str = "three_drug_half_dose_efficacy"
    THREE_DRUGS_FULL_DOSE: str = "three_drug_std_dose_efficacy"

    @property
    def name(self):
        return "sbp_medication_level"


SBP_MEDICATION_LEVEL = __SBPMedicationLevel()


class __LDLCMedicationLevel(NamedTuple):
    """high ldl-c medication level"""

    LOW: str = "low_intensity"
    MED: str = "medium_intensity"
    LOW_MED_EZE: str = "low_med_with_eze"
    HIGH: str = "high_intensity"
    HIGH_EZE: str = "high_with_eze"

    @property
    def name(self):
        return "ldlc_medication_level"


LDLC_MEDICATION_LEVEL = __LDLCMedicationLevel()

MEDICATION_RAMP = {
    "sbp": {
        SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE: 1,
        SBP_MEDICATION_LEVEL.ONE_DRUG_FULL_DOSE: 2,
        SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE: 3,
        SBP_MEDICATION_LEVEL.TWO_DRUGS_FULL_DOSE: 4,
        SBP_MEDICATION_LEVEL.THREE_DRUGS_HALF_DOSE: 5,
        SBP_MEDICATION_LEVEL.THREE_DRUGS_FULL_DOSE: 6,
    },
    "ldlc": {
        LDLC_MEDICATION_LEVEL.LOW: 1,
        LDLC_MEDICATION_LEVEL.MED: 2,
        LDLC_MEDICATION_LEVEL.LOW_MED_EZE: 3,
        LDLC_MEDICATION_LEVEL.HIGH: 4,
        LDLC_MEDICATION_LEVEL.HIGH_EZE: 5,
    },
}


# Define the baseline medication ramp level for simulants who are initialized as medicated
BASELINE_MEDICATION_LEVEL_PROBABILITY = {
    "sbp": {
        MEDICATION_RAMP["sbp"][SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE]: 0.57,
        MEDICATION_RAMP["sbp"][SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE]: 0.43,
    },
    "ldlc": {
        MEDICATION_RAMP["ldlc"][LDLC_MEDICATION_LEVEL.LOW]: 0.0382,
        MEDICATION_RAMP["ldlc"][LDLC_MEDICATION_LEVEL.MED]: 0.7194,
        MEDICATION_RAMP["ldlc"][LDLC_MEDICATION_LEVEL.HIGH]: 0.2424,
    },
}


# Define first-prescribed medication ramp levels for simulants who overcome therapeutic inertia
FIRST_PRESCRIPTION_LEVEL_PROBABILITY = {
    "sbp": {
        "high": {
            MEDICATION_RAMP["sbp"][SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE]: 0.55,
            MEDICATION_RAMP["sbp"][SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE]: 0.45,
        },
    },
}


class __MedicationAdherenceType(NamedTuple):
    """medication adherence types"""

    ADHERENT: str = "adherent"
    PRIMARY_NON_ADHERENT: str = "primary_non_adherent"
    SECONDARY_NON_ADHERENT: str = "secondary_non_adherent"

    @property
    def name(self):
        return "medication_adherence_type"


MEDICATION_ADHERENCE_TYPE = __MedicationAdherenceType()


# Define medication adherence level probabilitiies
MEDICATION_ADHERENCE_TYPE_PROBABILITIY = {
    "sbp": {
        MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.7392,
        MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.16,
        MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.1008,
    },
    "ldlc": {
        MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.6525,
        MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.25,
        MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.0975,
    },
}


class __MedicationAdherenceScore(NamedTuple):
    """adherence scores; used in medication treatment effect calculation"""

    ADHERENT: float = 1.0
    PRIMARY_NON_ADHERENT: float = 0.0
    SECONDARY_NON_ADHERENT: float = 0.0

    @property
    def name(self):
        return "medication_adherence_score"


MEDICATION_ADHERENCE_SCORE = __MedicationAdherenceScore()


BASELINE_MEDICATION_COVERAGE_SEX_MAPPING = {
    # used in medication treatment effect calculation
    "Female": 2,
    "Male": 1,
}


class MedicationCoverageCoefficientsBaseClass(NamedTuple):
    """Base class to define medication coverage coefficients"""

    NAME: str
    INTERCEPT: float
    SBP: float
    LDLC: float
    AGE: float
    SEX: float

    @property
    def name(self):
        return "medication_coverage_coefficients_base_class"


class __MedicationCoveragecoefficients(NamedTuple):
    """Coefficients used to calculate baseline medication coverage upon initialization"""

    SBP: MedicationCoverageCoefficientsBaseClass = MedicationCoverageCoefficientsBaseClass(
        "sbp", -6.75, 0.025, -0.0045, 0.05, 0.16
    )
    LDLC: MedicationCoverageCoefficientsBaseClass = MedicationCoverageCoefficientsBaseClass(
        "ldlc", -4.23, -0.0026, -0.005, 0.062, -0.19
    )
    BOTH: MedicationCoverageCoefficientsBaseClass = MedicationCoverageCoefficientsBaseClass(
        "both", -6.26, 0.018, -0.014, 0.069, 0.13
    )

    @property
    def name(self):
        return "medication coverage coefficients"


MEDICATION_COVERAGE_COEFFICIENTS = __MedicationCoveragecoefficients()


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
