from typing import NamedTuple

from vivarium_nih_us_cvd.utilities import get_norm

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
    SBP_MULTIPLIER: str = "sbp_multiplier"
    LDLC_MULTIPLIER: str = "ldlc_multiplier"
    OUTREACH: str = "outreach"
    POLYPILL: str = "polypill"

    @property
    def name(self):
        return "columns"


COLUMNS = __Columns()


##################
# Pipeline names #
##################


class __Pipelines(NamedTuple):
    """value pipeline names"""

    SBP_GBD_EXPOSURE: str = "high_systolic_blood_pressure.gbd_exposure"
    SBP_EXPOSURE: str = "high_systolic_blood_pressure.exposure"
    LDLC_GBD_EXPOSURE: str = "high_ldl_cholesterol.gbd_exposure"
    LDLC_EXPOSURE: str = "high_ldl_cholesterol.exposure"
    SBP_MEDICATION_ADHERENCE_EXPOSURE: str = "sbp_medication_adherence.exposure"
    LDLC_MEDICATION_ADHERENCE_EXPOSURE: str = "ldlc_medication_adherence.exposure"
    OUTREACH_EXPOSURE: str = "outreach.exposure"
    POLYPILL_EXPOSURE: str = "polypill.exposure"

    @property
    def name(self):
        return "pipelines"


PIPELINES = __Pipelines()


########################
# Component priorities #
########################


class __TimestepCleanupPriorities(NamedTuple):
    """component timestep cleanup listener priorities"""

    HEALTHCARE_VISITS: int = 5
    TREATMENT: int = 6

    @property
    def name(self):
        return "timestep_cleanup_priorities"


TIMESTEP_CLEANUP_PRIORITIES = __TimestepCleanupPriorities()


#####################################
# Healthcare Utilization Parameters #
#####################################

FOLLOWUP_MIN = 3 * 30  # 3 months
FOLLOWUP_MAX = 6 * 30  # 6 months

MISS_SCHEDULED_VISIT_PROBABILITY = 0.0868

MEASUREMENT_ERROR_MEAN_SBP = 0  # mmHg
MEASUREMENT_ERROR_SD_SBP = 2.9  # mmHg
MEASUREMENT_ERROR_MEAN_LDLC = 0  # mmol/L
MEASUREMENT_ERROR_SD_LDLC = 0.08  # mmol/L


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

# Therapeutic inertias (probability that a patient will not have their medication changed)
SBP_THERAPEUTIC_INERTIA = 0.4176
LDLC_THERAPEUTIC_INERTIA = 0.194


class __SBPThreshold(NamedTuple):
    """sbp exposure thresholds"""

    LOW: float = 130
    HIGH: float = 140

    @property
    def name(self):
        return "sbp_threshold"


SBP_THRESHOLD = __SBPThreshold()


class __ASCVDCoefficients(NamedTuple):
    """ACSVD coefficients"""

    INTERCEPT: float = -19.5
    SBP: float = 0.043
    AGE: float = 0.266
    SEX: float = 2.32

    @property
    def name(self):
        return "ascvd_coefficients"


ASCVD_COEFFICIENTS = __ASCVDCoefficients()


ASCVD_SEX_MAPPING = {
    # used in ASCVD calculation
    "Female": 0,
    "Male": 1,
}


class __ASCVDThreshold(NamedTuple):
    """ASCVD thresholds"""

    LOW: float = 7.5
    HIGH: float = 20

    @property
    def name(self):
        return "ascvd_threshold"


ASCVD_THRESHOLD = __ASCVDThreshold()


class __LDLCThreshold(NamedTuple):
    """ldl-c exposure thresholds"""

    LOW: float = 1.81
    HIGH: float = 4.91

    @property
    def name(self):
        return "ldlc_threshold"


LDLC_THRESHOLD = __LDLCThreshold()


class MedicationRampBaseClass(NamedTuple):
    """Base class to define medication levels and ramp values"""

    DESCRIPTION: str
    VALUE: int

    @property
    def name(self):
        return "medication_ramp_base_class"


class __SBPMedicationLevel(NamedTuple):
    """high sbp medication level"""

    NO_TREATMENT: MedicationRampBaseClass = MedicationRampBaseClass("no_treatment", 0)
    ONE_DRUG_HALF_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "one_drug_half_dose_efficacy", 1
    )
    ONE_DRUG_FULL_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "one_drug_std_dose_efficacy", 2
    )
    TWO_DRUGS_HALF_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "two_drug_half_dose_efficacy", 3
    )
    TWO_DRUGS_FULL_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "two_drug_std_dose_efficacy", 4
    )
    THREE_DRUGS_HALF_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "three_drug_half_dose_efficacy", 5
    )
    THREE_DRUGS_FULL_DOSE: MedicationRampBaseClass = MedicationRampBaseClass(
        "three_drug_std_dose_efficacy", 6
    )

    @property
    def name(self):
        return "sbp_medication_level"


SBP_MEDICATION_LEVEL = __SBPMedicationLevel()


class __SBPMultiplier(NamedTuple):
    """gbd SBP multipliers to convert to untreated values"""

    ONE_DRUG: float = 1.051
    TWO_DRUGS: float = 1.12

    @property
    def name(self):
        return "sbp_multiplier"


SBP_MULTIPLIER = __SBPMultiplier()


class __LDLCMultiplier(NamedTuple):
    """gbd LDLC multipliers to convert to untreated values"""

    LOW: float = 1.2467
    MED: float = 1.362
    HIGH: float = 1.5125

    @property
    def name(self):
        return "ldlc_multiplier"


LDLC_MULTIPLIER = __LDLCMultiplier()


class __LDLCMedicationLevel(NamedTuple):
    """high ldl-c medication level"""

    NO_TREATMENT: MedicationRampBaseClass = MedicationRampBaseClass("no_treatment", 0)
    LOW: MedicationRampBaseClass = MedicationRampBaseClass("low_intensity", 1)
    MED: MedicationRampBaseClass = MedicationRampBaseClass("medium_intensity", 2)
    LOW_MED_EZE: MedicationRampBaseClass = MedicationRampBaseClass("low_med_with_eze", 3)
    HIGH: MedicationRampBaseClass = MedicationRampBaseClass("high_intensity", 4)
    HIGH_EZE: MedicationRampBaseClass = MedicationRampBaseClass("high_with_eze", 5)

    @property
    def name(self):
        return "ldlc_medication_level"


LDLC_MEDICATION_LEVEL = __LDLCMedicationLevel()


class LDLCMedicationEfficacyBaseClass(NamedTuple):
    """Base class to define ldl-c medication efficacy parameters"""

    DESCRIPTION: float
    SEEDED_DISTRIBUTION: tuple

    @property
    def name(self):
        return "ldlc_medication_efficacy_base_class"


class __LDLCMedicatonEfficacy(NamedTuple):
    """high ldl-c medication efficacy"""

    LOW: LDLCMedicationEfficacyBaseClass = LDLCMedicationEfficacyBaseClass(
        LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION,
        ("ldlc_medication_efficacy_low", get_norm(mean=24.67, sd=1.2224)),
    )
    MED: LDLCMedicationEfficacyBaseClass = LDLCMedicationEfficacyBaseClass(
        LDLC_MEDICATION_LEVEL.MED.DESCRIPTION,
        ("ldlc_medication_efficacy_med", get_norm(mean=36.2, sd=1.4031)),
    )
    HIGH: LDLCMedicationEfficacyBaseClass = LDLCMedicationEfficacyBaseClass(
        LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION,
        ("ldlc_medication_efficacy_high", get_norm(mean=51.25, sd=2.179)),
    )
    LOW_MED_EZE: LDLCMedicationEfficacyBaseClass = LDLCMedicationEfficacyBaseClass(
        LDLC_MEDICATION_LEVEL.LOW_MED_EZE.DESCRIPTION,
        ("ldlc_medication_efficacy_low_med_eze", get_norm(mean=46.1, sd=1.4031)),
    )
    HIGH_EZE: LDLCMedicationEfficacyBaseClass = LDLCMedicationEfficacyBaseClass(
        LDLC_MEDICATION_LEVEL.HIGH_EZE.DESCRIPTION,
        ("ldlc_medication_efficacy_high_eze", get_norm(mean=61.15, sd=2.179)),
    )

    @property
    def name(self):
        return "ldlc_medication_efficacy"


LDLC_MEDICATION_EFFICACY = __LDLCMedicatonEfficacy()


# Define the baseline medication ramp level for simulants who are initialized as medicated
BASELINE_MEDICATION_LEVEL_PROBABILITY = {
    "sbp": {
        SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE.DESCRIPTION: 0.57,
        SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE.DESCRIPTION: 0.43,
    },
    "ldlc": {
        LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION: 0.0382,
        LDLC_MEDICATION_LEVEL.MED.DESCRIPTION: 0.7194,
        LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION: 0.2424,
    },
}


# Define first-prescribed medication ramp levels for simulants who overcome therapeutic inertia
FIRST_PRESCRIPTION_LEVEL_PROBABILITY = {
    "sbp": {
        "high": {
            SBP_MEDICATION_LEVEL.ONE_DRUG_HALF_DOSE.DESCRIPTION: 0.55,
            SBP_MEDICATION_LEVEL.TWO_DRUGS_HALF_DOSE.DESCRIPTION: 0.45,
        },
    },
    "ldlc": {
        # [Treatment ramp ID D] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, and
        # have a history of MI or IS
        "ramp_id_d": {
            LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION: 0.06,
            LDLC_MEDICATION_LEVEL.MED.DESCRIPTION: 0.52,
            LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION: 0.42,
        },
        # [Treatment ramp ID E] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, have
        # no history of MI or IS, and who have high LDLC or ASCVD
        "ramp_id_e": {
            LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION: 0.10,
            LDLC_MEDICATION_LEVEL.MED.DESCRIPTION: 0.66,
            LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION: 0.24,
        },
        # [Treatment ramp ID F] Simulants who overcome therapeutic inertia, have
        # elevated LDLC, are not currently medicated, have elevated ASCVD, have
        # no history of MI or IS, but who do NOT have high LDLC or ASCVD
        "ramp_id_f": {
            LDLC_MEDICATION_LEVEL.LOW.DESCRIPTION: 0.14,
            LDLC_MEDICATION_LEVEL.MED.DESCRIPTION: 0.71,
            LDLC_MEDICATION_LEVEL.HIGH.DESCRIPTION: 0.15,
        },
    },
}


class __MedicationAdherenceType(NamedTuple):
    """medication adherence types"""

    PRIMARY_NON_ADHERENT: str = "cat1"
    SECONDARY_NON_ADHERENT: str = "cat2"
    ADHERENT: str = "cat3"

    @property
    def name(self):
        return "medication_adherence_type"


MEDICATION_ADHERENCE_TYPE = __MedicationAdherenceType()


MEDICATION_ADHERENCE_CATEGORY_MAPPING = {
    MEDICATION_ADHERENCE_TYPE.ADHERENT: "adherent",
    MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: "secondary_non_adherent",
    MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: "primary_non_adherent",
}


# Define medication adherence level probabilitiies
MEDICATION_ADHERENCE_TYPE_PROBABILITIY = {
    "sbp_medication_adherence": {
        MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.16,
        MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.1008,
        MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.7392,
    },
    "ldlc_medication_adherence": {
        MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.25,
        MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.0975,
        MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.6525,
    },
}


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
        "sbp", -6.75, 0.025, -0.173, 0.05, 0.158
    )
    LDLC: MedicationCoverageCoefficientsBaseClass = MedicationCoverageCoefficientsBaseClass(
        "ldlc", -4.23, -0.0026, -0.196, 0.062, -0.19
    )
    BOTH: MedicationCoverageCoefficientsBaseClass = MedicationCoverageCoefficientsBaseClass(
        "both", -6.26, 0.018, -0.524, 0.069, 0.13
    )

    @property
    def name(self):
        return "medication_coverage_coefficients"


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
    "high_body_mass_index_in_adults": {
        "minimum": 5,
        "maximum": 80,
    },
}


###########################
# Intervention Parameters #
###########################

INTERVENTION_CATEGORY_MAPPING = {
    "cat2": "no",
    "cat1": "yes",
}


class __OutreachEffectSBP(NamedTuple):
    """outreach effect on sbp medication primary_non_adherent levels"""

    TO_ADHERENT: float = 0.4455
    TO_SECONDARY_NON_ADHERENT: float = 0.0608
    NO_CHANGE: float = 0.4937

    @property
    def name(self):
        return "sbp_multiplier"


OUTREACH_EFFECT_SBP = __OutreachEffectSBP()


class __OutreachEffectLDLC(NamedTuple):
    """outreach effect on ldlc medication primary_non_adherent levels"""

    TO_ADHERENT: float = 0.4653
    TO_SECONDARY_NON_ADHERENT: float = 0.0695
    NO_CHANGE: float = 0.4652

    @property
    def name(self):
        return "sbp_multiplier"


OUTREACH_EFFECT_LDLC = __OutreachEffectLDLC()


# Define the outreach effects on primary_non_adherence (cat 1) levels
OUTREACH_EFFECTS = {
    "sbp": {
        "cat3": OUTREACH_EFFECT_SBP.TO_ADHERENT,
        "cat2": OUTREACH_EFFECT_SBP.TO_SECONDARY_NON_ADHERENT,
        "cat1": OUTREACH_EFFECT_SBP.NO_CHANGE,
    },
    "ldlc": {
        "cat3": OUTREACH_EFFECT_LDLC.TO_ADHERENT,
        "cat2": OUTREACH_EFFECT_LDLC.TO_SECONDARY_NON_ADHERENT,
        "cat1": OUTREACH_EFFECT_LDLC.NO_CHANGE,
    },
}

# Define the polypill effect sbp medication adherence coverage
POLYPILL_SBP_MEDICATION_ADHERENCE_COVERAGE = {
    MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT: 0.16,
    MEDICATION_ADHERENCE_TYPE.SECONDARY_NON_ADHERENT: 0.049,
    MEDICATION_ADHERENCE_TYPE.ADHERENT: 0.791,
}
