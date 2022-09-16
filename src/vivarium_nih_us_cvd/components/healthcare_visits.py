from typing import Dict

import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, data_values, models


class HealthcareVisits:
    """Manages healthcare utilization"""

    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self):
        pass

    def __repr__(self):
        return "HealthcareVisits"

    def setup(self, builder: Builder) -> None:
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
        self.randomness = builder.randomness.get_stream(self.name)

        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(
            utilization_data, parameter_columns=["age", "year"], key_columns=["sex"]
        )
        self.background_utilization_rate = builder.value.register_rate_producer(
            "utilization_rate", background_utilization_rate, requires_columns=["age", "sex"]
        )

        # Add columns
        self.visit_type_column = data_values.VISIT_TYPE_COLUMN
        self.scheduled_visit_date_column = data_values.SCHEDULED_VISIT_DATE_COLUMN
        self.miss_scheduled_visit_probability_column = (
            data_values.MISS_SCHEDULED_VISIT_PROBABILITY_COLUMN
        )
        columns_created = [
            self.visit_type_column,
            self.scheduled_visit_date_column,
            self.miss_scheduled_visit_probability_column,
        ]
        columns_required = [
            models.ISCHEMIC_STROKE_MODEL_NAME,
            models.MYOCARDIAL_INFARCTION_MODEL_NAME,
        ]
        self.population_view = builder.population.get_view(columns_required + columns_created)

        # Initialize simulants
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=columns_created,
            requires_columns=columns_required,
        )

        # Register listeners
        builder.event.register_listener("time_step__cleanup", self.on_time_step_cleanup)

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """All simulants on SBP medication, LDL-C medication, or a history of
        an acute event (ie intialized in state post-MI or chronic IS) should be
        initialized with a scheduled followup visit 0-6 months out, uniformly
        distributed. All simulants initialized in an acute state should be
        scheduled a followup visit 3-6 months out, uniformly distributed.

        For simplicity, do not assign background screenings on initialization.

        A burn-in period allows for the observed simulation to start with more
        realistic treatments and scheduled followups.
        """
        index = pop_data.index
        event_time = self.clock() + self.step_size()
        df = self.population_view.subview(
            [models.ISCHEMIC_STROKE_MODEL_NAME, models.MYOCARDIAL_INFARCTION_MODEL_NAME]
        ).get(pop_data.index)
        df[self.visit_type_column] = "none"
        df[self.scheduled_visit_date_column] = pd.NaT

        # Assign probabilities that each simulant will miss scheduled visits
        lower = data_values.MISS_SCHEDULED_VISIT_PROBABILITY_MIN
        upper = data_values.MISS_SCHEDULED_VISIT_PROBABILITY_MAX
        df[self.miss_scheduled_visit_probability_column] = lower + (
            upper - lower
        ) * self.randomness.get_draw(index)

        # Handle simulants initialized in an post/chronic state
        visitors = {}
        mask_chronic_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = index[mask_chronic_is | mask_post_mi]
        visitors[data_values.VISIT_TYPES.SCHEDULED] = acute_history
        df.loc[acute_history] = self.visit_doctor(
            df=df.loc[acute_history],
            visitors=visitors,
            event_time=event_time,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )
        # Handle simulants initialized in an emergency state
        visitors = {}
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        emergency_not_already_scheduled = index[
            (mask_acute_is | mask_acute_mi) & ~(mask_chronic_is | mask_post_mi)
        ]
        visitors[data_values.VISIT_TYPES.EMERGENCY] = emergency_not_already_scheduled
        df.loc[emergency_not_already_scheduled] = self.visit_doctor(
            df=df.loc[emergency_not_already_scheduled],
            visitors=visitors,
            event_time=event_time,
        )

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
                    self.miss_scheduled_visit_probability_column,
                ]
            ]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit. In the event that a simulant goes for an emergency or
        background visit but already has a followup visit scheduled for the future,
        keep that scheduled followup and do not schedule another one.
        """
        df = self.population_view.get(event.index, query='alive == "alive"')
        visitors = {}

        # Emergency visits
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        visit_emergency = df[mask_emergency].index
        visitors[data_values.VISIT_TYPES.EMERGENCY] = visit_emergency

        # Scheduled visits
        mask_scheduled_non_emergency = (
            (df[self.scheduled_visit_date_column] > (event.time - event.step_size))
            & (df[self.scheduled_visit_date_column] <= event.time)
            & (~mask_emergency)
        )
        scheduled_non_emergency = df[mask_scheduled_non_emergency].index
        # Missed scheduled (non-emergency) visits (these do not get re-scheduled)
        missed_visit = self.randomness.filter_for_probability(
            scheduled_non_emergency,
            df.loc[scheduled_non_emergency, self.miss_scheduled_visit_probability_column],
        )  # pd.Index
        visitors[data_values.VISIT_TYPES.MISSED] = missed_visit
        df.loc[missed_visit, self.scheduled_visit_date_column] = pd.NaT  # no re-schedule
        visit_scheduled = scheduled_non_emergency.difference(missed_visit)
        visitors[data_values.VISIT_TYPES.SCHEDULED] = visit_scheduled

        # Background visits (for those who did not go for another reason (even missed screenings))
        maybe_background = df.index.difference(visit_emergency.union(visit_scheduled))
        utilization_rate = self.background_utilization_rate(maybe_background)
        visit_background = self.randomness.filter_for_rate(
            maybe_background, utilization_rate
        )  # pd.Index
        visitors[data_values.VISIT_TYPES.BACKGROUND] = visit_background

        df = self.visit_doctor(df, visitors=visitors, event_time=event.time)

        self.population_view.update(
            df[[self.visit_type_column, self.scheduled_visit_date_column]]
        )

    def visit_doctor(
        self,
        df: pd.DataFrame,
        visitors: Dict,
        event_time: pd.Timestamp,
        min_followup: int = data_values.FOLLOWUP_MIN,
        max_followup: int = data_values.FOLLOWUP_MAX,
    ) -> pd.DataFrame:
        """Updates treatment plans and schedules followups.

        Arguments:
            df: state table to update
            visitors: dictionary of all simulants visiting the doctor and their visit types
            event_time: timestamp at end of the time step
            min_followup: minimum number of days out to schedule followup
            max_followup: maximum number of days out to schedule followup
        """
        # Update visit type
        df[self.visit_type_column] = "none"
        for k in visitors:
            df.loc[visitors[k], self.visit_type_column] = df[self.visit_type_column] + f";{k}"

        # Update treatments
        to_visit = pd.Index([])
        attended_visit_types = [
            visit_type
            for visit_type in data_values.VISIT_TYPES
            if visit_type != data_values.VISIT_TYPES.MISSED
        ]
        for visit_type in attended_visit_types:
            to_visit = to_visit.union(visitors.get(visit_type, pd.Index([])))

        self.update_treatment(index=to_visit)

        # Update scheduled visits
        # Only schedule a followup if a future one does not already exist
        has_followup_scheduled = df.loc[
            to_visit.intersection(
                df[(df[self.scheduled_visit_date_column] > event_time)].index
            )
        ].index
        to_schedule_followup = to_visit.difference(has_followup_scheduled)

        df.loc[
            to_schedule_followup, self.scheduled_visit_date_column
        ] = self.schedule_followup(
            to_schedule_followup, event_time, min_followup, max_followup
        )

        return df

    def update_treatment(self, index: pd.Index):
        # TODO [MIC-3371, MIC-3375]
        pass

    def schedule_followup(
        self,
        index: pd.Index,
        event_time: pd.Timestamp,
        min_followup: int = data_values.FOLLOWUP_MIN,
        max_followup: int = data_values.FOLLOWUP_MAX,
    ) -> pd.Series:
        """Schedules followup visits"""
        return pd.Series(
            event_time
            + self.random_time_delta(
                pd.Series(min_followup, index=index),
                pd.Series(max_followup, index=index),
            ),
            index=index,
        )

    def random_time_delta(self, start: pd.Series, end: pd.Series) -> pd.Series:
        """Generate a random time delta for each individual in the start
        and end series."""
        return pd.to_timedelta(
            start + (end - start) * self.randomness.get_draw(start.index), unit="day"
        )
