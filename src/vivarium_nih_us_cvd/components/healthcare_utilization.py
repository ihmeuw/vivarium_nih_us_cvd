from typing import Dict, List, Optional

import pandas as pd
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.components.treatment import Treatment
from vivarium_nih_us_cvd.constants import data_keys, data_values, models


class HealthcareUtilization(Component):
    """Manages healthcare utilization and scheduling of appointments."""

    ##############
    # Properties #
    ##############

    @property
    def columns_created(self) -> List[str]:
        return [
            data_values.COLUMNS.VISIT_TYPE,
            data_values.COLUMNS.SCHEDULED_VISIT_DATE,
            data_values.COLUMNS.LAST_FPG_TEST_DATE,
        ]

    @property
    def columns_required(self) -> Optional[List[str]]:
        return [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
            data_values.COLUMNS.LIFESTYLE,
        ]

    @property
    def initialization_requirements(self) -> Dict[str, List[str]]:
        return {
            "requires_columns": self.columns_required,
            "requires_values": [
                data_values.PIPELINES.SBP_EXPOSURE,
                data_values.PIPELINES.LDLC_EXPOSURE,
            ],
            "requires_streams": [self.name],
        }

    @property
    def time_step_cleanup_priority(self) -> int:
        return data_values.TIMESTEP_CLEANUP_PRIORITIES.HEALTHCARE_VISITS

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self):
        super().__init__()
        self.treatment = Treatment()
        self._sub_components = [self.treatment]

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)

        self.lifestyle = builder.value.get_value(data_values.PIPELINES.LIFESTYLE_EXPOSURE)
        self.bmi_raw = builder.value.get_value(data_values.PIPELINES.BMI_RAW_EXPOSURE)
        self.bmi = builder.value.get_value(data_values.PIPELINES.BMI_EXPOSURE)
        self.fpg = builder.value.get_value(data_values.PIPELINES.FPG_EXPOSURE)

        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(
            utilization_data, parameter_columns=["age", "year"], key_columns=["sex"]
        )
        self.background_utilization_rate = builder.value.register_rate_producer(
            "utilization_rate", background_utilization_rate, requires_columns=["age", "sex"]
        )

    ########################
    # Event-driven methods #
    ########################

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """Initializes the visit type of simulants in an emergency state so
        that they can be sent to the medication ramps. A burn-in period allows
        for the observed simulation to start with more realistic scheduled followups.

        This component also initializes scheduled followups. All simulants on
        SBP medication, LDL-C medication, or a history of an acute event
        (ie intialized in state post-MI or chronic IS) should be initialized with
        a scheduled followup visit 0-6 months out, uniformly distributed. All
        simulants initialized in an acute state should be scheduled a followup
        visit 3-6 months out, uniformly distributed.
        """
        event_time = self.clock() + self.step_size()
        pop = self.population_view.subview(
            [
                "age",
                "sex",
                models.ISCHEMIC_STROKE_MODEL_NAME,
                models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME,
                data_values.COLUMNS.SBP_MEDICATION,
                data_values.COLUMNS.LDLC_MEDICATION,
            ]
        ).get(pop_data.index)

        # Initialize new columns
        pop[data_values.COLUMNS.VISIT_TYPE] = data_values.VISIT_TYPE.NONE
        pop[data_values.COLUMNS.SCHEDULED_VISIT_DATE] = pd.NaT

        # Update simulants initialized in an emergency state
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        emergency = pop[(mask_acute_is | mask_acute_mi)].index
        pop.loc[emergency, data_values.COLUMNS.VISIT_TYPE] = data_values.VISIT_TYPE.EMERGENCY

        # Schedule followups
        # On medication and/or Post/chronic state 0-6 months out
        on_medication = pop[
            (
                pop[data_values.COLUMNS.SBP_MEDICATION]
                != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
            )
            | (
                pop[data_values.COLUMNS.LDLC_MEDICATION]
                != data_values.LDLC_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
            )
        ].index
        mask_chronic_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME]
            == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            pop[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = pop[mask_chronic_is | mask_post_mi].index
        pop.loc[
            on_medication.union(acute_history), data_values.COLUMNS.SCHEDULED_VISIT_DATE
        ] = self.schedule_followup(
            index=acute_history,
            event_time=event_time,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )
        # Emergency (acute) state 3-6 months out
        pop.loc[emergency, data_values.COLUMNS.SCHEDULED_VISIT_DATE] = self.schedule_followup(
            index=emergency,
            event_time=event_time,
        )

        # Generate column for last FPG test date
        bmi = self.bmi_raw(pop.index)
        age = pop["age"]
        is_eligible_for_testing = (
            age >= data_values.FPG_TESTING.AGE_ELIGIBILITY_THRESHOLD
        ) & (bmi >= data_values.FPG_TESTING.BMI_ELIGIBILITY_THRESHOLD)

        # Determine which eligible simulants get assigned a test date
        simulants_with_test_date = self.randomness.filter_for_probability(
            pop[is_eligible_for_testing],
            [data_values.FPG_TESTING.PROBABILITY_OF_TESTING_GIVEN_ELIGIBLE]
            * sum(is_eligible_for_testing),
        )

        # sample from dates uniformly distributed from 0 to 3 years before sim start date
        draws = self.randomness.get_draw(
            index=simulants_with_test_date.index, additional_key="fpg_test_date"
        )
        time_before_event_start = draws * pd.Timedelta(
            days=365.25 * data_values.FPG_TESTING.MIN_YEARS_BETWEEN_TESTS
        )

        fpg_test_date_column = pd.Series(pd.NaT, index=pop.index)
        fpg_test_date_column[simulants_with_test_date.index] = (
            self.clock() + pd.Timedelta(days=28) - time_before_event_start
        )
        pop[data_values.COLUMNS.LAST_FPG_TEST_DATE] = fpg_test_date_column

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.VISIT_TYPE,
                    data_values.COLUMNS.SCHEDULED_VISIT_DATE,
                    data_values.COLUMNS.LAST_FPG_TEST_DATE,
                ]
            ]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit and schedule followups. In the event that a simulant
        already has a followup visit scheduled for the future, keep that scheduled
        followup and do not schedule a new one or another one.
        """
        event_time = event.time
        pop = self.population_view.get(event.index, query='alive == "alive"')
        pop[data_values.COLUMNS.VISIT_TYPE] = data_values.VISIT_TYPE.NONE

        # Emergency visits
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.ISCHEMIC_HEART_DISEASE_AND_HEART_FAILURE_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        visit_emergency = pop[mask_emergency].index
        pop.loc[
            visit_emergency, data_values.COLUMNS.VISIT_TYPE
        ] = data_values.VISIT_TYPE.EMERGENCY

        # Missed scheduled (non-emergency) visits (these do not get re-scheduled)
        mask_scheduled_non_emergency = (
            pop[data_values.COLUMNS.SCHEDULED_VISIT_DATE] <= event_time
        ) & (~mask_emergency)
        scheduled_non_emergency = pop[mask_scheduled_non_emergency].index
        pop.loc[scheduled_non_emergency, data_values.COLUMNS.SCHEDULED_VISIT_DATE] = pd.NaT
        visit_missed = scheduled_non_emergency[
            self.randomness.get_draw(
                scheduled_non_emergency, additional_key="miss_scheduled_visits"
            )
            <= data_values.MISS_SCHEDULED_VISIT_PROBABILITY
        ]
        pop.loc[visit_missed, data_values.COLUMNS.VISIT_TYPE] = data_values.VISIT_TYPE.MISSED

        # Scheduled visits
        visit_scheduled = scheduled_non_emergency.difference(visit_missed)
        pop.loc[
            visit_scheduled, data_values.COLUMNS.VISIT_TYPE
        ] = data_values.VISIT_TYPE.SCHEDULED

        # Background visits (for those who did not go for another reason or miss their scheduled visit)
        maybe_background = pop.index.difference(
            visit_emergency.union(scheduled_non_emergency)
        )
        utilization_rate = self.background_utilization_rate(maybe_background)
        visit_background = self.randomness.filter_for_rate(
            maybe_background, utilization_rate, additional_key="background_visits"
        )  # pd.Index
        pop.loc[
            visit_background, data_values.COLUMNS.VISIT_TYPE
        ] = data_values.VISIT_TYPE.BACKGROUND

        # Test FPG and enroll in lifestyle
        all_visitors = visit_emergency.union(visit_scheduled).union(visit_background)
        tested_simulants = self.test_fpg(pop_visitors=pop.loc[all_visitors])
        newly_lifestyle_enrolled_simulants = self.determine_lifestyle_enrollment(
            tested_simulants=tested_simulants
        )

        pop.loc[tested_simulants.index, data_values.COLUMNS.LAST_FPG_TEST_DATE] = self.clock()
        pop.loc[
            newly_lifestyle_enrolled_simulants, data_values.COLUMNS.LIFESTYLE
        ] = self.clock()

        # Schedule followups
        needs_followup_sbp = self.determine_followups_sbp(pop_visitors=pop.loc[all_visitors])
        needs_followup_ldlc = self.determine_followups_ldlc(
            pop_visitors=pop.loc[all_visitors]
        )
        # Do not schedule a followup if one already exists
        has_followup_already_scheduled = pop[
            (pop[data_values.COLUMNS.SCHEDULED_VISIT_DATE] > event_time)
        ].index

        to_schedule_followup = newly_lifestyle_enrolled_simulants.union(
            (needs_followup_sbp.union(needs_followup_ldlc))
        ).difference(has_followup_already_scheduled)
        pop.loc[
            to_schedule_followup, data_values.COLUMNS.SCHEDULED_VISIT_DATE
        ] = self.schedule_followup(index=to_schedule_followup, event_time=event_time)

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.LAST_FPG_TEST_DATE,
                    data_values.COLUMNS.LIFESTYLE,
                    data_values.COLUMNS.VISIT_TYPE,
                    data_values.COLUMNS.SCHEDULED_VISIT_DATE,
                ]
            ]
        )

    ##################
    # Helper methods #
    ##################

    def test_fpg(self, pop_visitors: pd.DataFrame) -> pd.DataFrame:
        not_already_enrolled = pop_visitors[data_values.COLUMNS.LIFESTYLE].isna()
        bmi = self.bmi(pop_visitors.index)
        age = pop_visitors["age"]

        fpg_not_tested_recently = (
            # never been tested for FPG
            pop_visitors[data_values.COLUMNS.LAST_FPG_TEST_DATE].isna()
        ) | (
            # last FPG test more than 3 years ago
            pop_visitors[data_values.COLUMNS.LAST_FPG_TEST_DATE]
            < self.clock()
            - pd.Timedelta(days=365.25 * data_values.FPG_TESTING.MIN_YEARS_BETWEEN_TESTS)
        )

        is_eligible_for_testing = (
            (not_already_enrolled)
            & (age >= data_values.FPG_TESTING.AGE_ELIGIBILITY_THRESHOLD)
            & (bmi >= data_values.FPG_TESTING.BMI_ELIGIBILITY_THRESHOLD)
            & (fpg_not_tested_recently)
        )

        # Determine which simulants eligible for FPG testing actually get tested
        tested_simulants = self.randomness.filter_for_probability(
            pop_visitors[is_eligible_for_testing],
            [data_values.FPG_TESTING.PROBABILITY_OF_TESTING_GIVEN_ELIGIBLE]
            * sum(is_eligible_for_testing),
        )

        return tested_simulants

    def determine_followups_sbp(self, pop_visitors: pd.DataFrame) -> pd.Index:
        """Apply SBP treatment ramp logic to determine who gets scheduled a followup"""
        visitors = pop_visitors.index
        measured_sbp = self.treatment.get_measured_sbp(index=visitors)
        visitors_high_sbp = visitors.intersection(
            measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.LOW].index
        )
        visitors_on_sbp_medication = visitors.intersection(
            pop_visitors[
                pop_visitors[data_values.COLUMNS.SBP_MEDICATION]
                != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
            ].index
        )
        # Schedule those on sbp medication or those not on sbp medication but have a high sbp
        needs_followup = visitors_on_sbp_medication.union(visitors_high_sbp)

        return needs_followup

    def determine_followups_ldlc(self, pop_visitors: pd.DataFrame) -> pd.Index:
        """Apply LDL-C treatment ramp logic to determine who gets scheduled a followup"""
        visitors = pop_visitors.index
        ascvd = self.treatment.get_ascvd(pop_visitors=pop_visitors)
        measured_ldlc = self.treatment.get_measured_ldlc(index=visitors)
        visitors_high_ascvd = visitors.intersection(
            ascvd[ascvd >= data_values.ASCVD_THRESHOLD.LOW].index
        )
        visitors_high_ldlc = visitors.intersection(
            measured_ldlc[measured_ldlc >= data_values.LDLC_THRESHOLD.LOW].index
        )

        # Schedule those with high ldlc and high ASCVD
        # All simulants under these conditions get scheduled a followup in our LDL-C ramp
        # regardless of medication status or medical history, and all simulants who don't meet
        # both conditions do not get scheduled a followup
        needs_followup = visitors_high_ascvd.intersection(visitors_high_ldlc)

        return needs_followup

    def determine_lifestyle_enrollment(self, tested_simulants: pd.DataFrame) -> pd.Index:
        """Apply lifestyle intervention ramp logic to determine who gets enrolled"""
        # FPG related ramping
        fpg = self.fpg(tested_simulants.index)
        fpg_within_bounds = (fpg >= data_values.FPG_TESTING.LOWER_ENROLLMENT_BOUND) & (
            fpg <= data_values.FPG_TESTING.UPPER_ENROLLMENT_BOUND
        )
        enroll_if_fpg_within_bounds = (
            self.lifestyle(tested_simulants.index) == data_values.LIFESTYLE_EXPOSURE.EXPOSED
        )
        newly_enrolled = fpg_within_bounds & enroll_if_fpg_within_bounds

        return tested_simulants[newly_enrolled].index

    def schedule_followup(
        self,
        index: pd.Index,
        event_time: pd.Timestamp,
        min_followup: int = data_values.FOLLOWUP_MIN,
        max_followup: int = data_values.FOLLOWUP_MAX,
    ) -> pd.Series:
        """Schedules followup visits"""
        random_time_delta = pd.to_timedelta(
            pd.Series(min_followup, index=index)
            + (pd.Series(max_followup, index=index) - pd.Series(min_followup, index=index))
            * self.randomness.get_draw(
                pd.Series(min_followup, index=index).index, additional_key="schedule_followup"
            ),
            unit="day",
        )
        return pd.Series(event_time + random_time_delta, index=index)
