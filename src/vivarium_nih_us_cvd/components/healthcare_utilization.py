from typing import List

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.components.treatment import Treatment
from vivarium_nih_us_cvd.constants import data_keys, data_values, models


class HealthcareUtilization:
    """Manages healthcare utilization and scheduling of appointments."""

    configuration_defaults = {}

    def __init__(self):
        self.treatment = self._get_treatment_component()
        self._sub_components = [self.treatment]

    def __repr__(self):
        return "HealthcareUtilization"

    def _get_treatment_component(self) -> Treatment:
        return Treatment()

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def sub_components(self) -> List:
        return self._sub_components

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)
        self.sbp = builder.value.get_value(data_values.PIPELINES.SBP_EXPOSURE)
        self.ldlc = builder.value.get_value(data_values.PIPELINES.LDLC_EXPOSURE)

        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(
            utilization_data, parameter_columns=["age", "year"], key_columns=["sex"]
        )
        self.background_utilization_rate = builder.value.register_rate_producer(
            "utilization_rate", background_utilization_rate, requires_columns=["age", "sex"]
        )

        columns_created = [
            data_values.COLUMNS.VISIT_TYPE,
            data_values.COLUMNS.SCHEDULED_VISIT_DATE,
        ]

        columns_required = [
            "age",
            "sex",
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.MYOCARDIAL_INFARCTION_MODEL_NAME,
            data_values.COLUMNS.SBP_MEDICATION,
            data_values.COLUMNS.LDLC_MEDICATION,
        ]

        self.population_view = builder.population.get_view(columns_required + columns_created)

        values_required = [
            data_values.PIPELINES.SBP_EXPOSURE,
            data_values.PIPELINES.LDLC_EXPOSURE,
        ]

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
            requires_columns=columns_required,
            requires_values=values_required,
        )

        # Register listeners
        builder.event.register_listener(
            "time_step__cleanup",
            self.on_time_step_cleanup,
            priority=data_values.TIMESTEP_CLEANUP_PRIORITIES.HEALTHCARE_VISITS,
        )

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """Initializes the visit type of simulants in an emergency state so
        that they can be sent to the medication ramps. A burn-in period allows
        for the observed simulation to start with more realistic scheduled followups.

        This component also initializes scheduled followups. All simulants on
        SBP medication, LDL-C medication, or a history of an acute event
        (ie intialized in state post-MI or chronic IS) should be initialized with
        a scheduled followup visit 0-6 months out, uniformly distributed. All
        simulants initialized in an acute state should bescheduled a followup
        visit 3-6 months out, uniformly distributed.
        """
        event_time = self.clock() + self.step_size()
        pop = self.population_view.subview(
            [
                "age",
                "sex",
                models.ISCHEMIC_STROKE_MODEL_NAME,
                models.MYOCARDIAL_INFARCTION_MODEL_NAME,
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
            pop[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
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
            pop[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
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

        self.population_view.update(
            pop[[data_values.COLUMNS.VISIT_TYPE, data_values.COLUMNS.SCHEDULED_VISIT_DATE]]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit and schedule followups. In the event that a simulant
        already has a followup visit scheduled for the future, keep that scheduled
        followup and do not schedule a new one or another one.
        """
        pop = self.population_view.get(event.index, query='alive == "alive"')
        pop[data_values.COLUMNS.VISIT_TYPE] = data_values.VISIT_TYPE.NONE

        # Emergency visits
        mask_acute_is = (
            pop[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            pop[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        visit_emergency = pop[mask_emergency].index
        pop.loc[
            visit_emergency, data_values.COLUMNS.VISIT_TYPE
        ] = data_values.VISIT_TYPE.EMERGENCY

        # Missed scheduled (non-emergency) visits (these do not get re-scheduled)
        mask_scheduled_non_emergency = (
            pop[data_values.COLUMNS.SCHEDULED_VISIT_DATE] <= event.time
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

        # Take measurements (required to determine if a followup is required)
        all_visitors = visit_emergency.union(visit_scheduled).union(visit_background)
        measured_sbp = self.treatment.get_measured_sbp(
            index=all_visitors,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
        )

        # Schedule followups
        visitors_high_sbp = all_visitors.intersection(
            measured_sbp[measured_sbp >= data_values.SBP_THRESHOLD.LOW].index
        )
        visitors_on_sbp_medication = all_visitors.intersection(
            pop[
                pop[data_values.COLUMNS.SBP_MEDICATION]
                != data_values.SBP_MEDICATION_LEVEL.NO_TREATMENT.DESCRIPTION
            ].index
        )
        # Schedule those on sbp medication or those not on sbp medication but have a high sbp
        needs_followup = visitors_on_sbp_medication.union(visitors_high_sbp)
        # Do no re-schedule followups that already exist
        has_followup_already_scheduled = pop[
            (pop[data_values.COLUMNS.SCHEDULED_VISIT_DATE] > event.time)
        ].index
        to_schedule_followup = needs_followup.difference(has_followup_already_scheduled)
        pop.loc[
            to_schedule_followup, data_values.COLUMNS.SCHEDULED_VISIT_DATE
        ] = self.schedule_followup(index=to_schedule_followup, event_time=event.time)

        self.population_view.update(
            pop[
                [
                    data_values.COLUMNS.VISIT_TYPE,
                    data_values.COLUMNS.SCHEDULED_VISIT_DATE,
                ]
            ]
        )

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
