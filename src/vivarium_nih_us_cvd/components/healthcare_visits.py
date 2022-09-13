import pandas as pd
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, models
from vivarium_nih_us_cvd.constants.data_values import VISIT_TYPE

FOLLOWUP_MIN = 3 * 30  # 3 months
FOLLOWUP_MAX = 6 * 30  # 6 months


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
        self.scheduled_visit_date_column = VISIT_TYPE.SCHEDULED_COLUMN_NAME
        columns_created = [self.scheduled_visit_date_column]
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
        scheduled a followup visit 3-6 months out.

        For simplicity, do not assign background screenings on initialization.

        A burn-in period will allow the sim to start the observed
        time period with more realistic boundary conditions.
        """
        idx = pop_data.index
        event_time = self.clock() + self.step_size()
        scheduled_dates = pd.Series(pd.NaT, index=idx, name=self.scheduled_visit_date_column)
        states = self.population_view.subview(
            [models.ISCHEMIC_STROKE_MODEL_NAME, models.MYOCARDIAL_INFARCTION_MODEL_NAME]
        ).get(pop_data.index)

        # Handle simulants initialized in a post/chronic state
        mask_chronic_is = (
            states[models.ISCHEMIC_STROKE_MODEL_NAME]
            == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            states[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_acute_history = mask_chronic_is | mask_post_mi
        scheduled_dates = self.visit_doctor(
            scheduled_dates=scheduled_dates,
            event_time=event_time,
            mask=mask_acute_history,
            min_followup=0,
            max_followup=FOLLOWUP_MAX - FOLLOWUP_MIN,
        )

        # Handle simulants initialized in an emergency state
        mask_acute_is = (
            states[models.ISCHEMIC_STROKE_MODEL_NAME]
            == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            states[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_initialized_acute = (mask_acute_is | mask_acute_mi) & ~mask_acute_history
        scheduled_dates = self.visit_doctor(
            scheduled_dates=scheduled_dates,
            event_time=event_time,
            mask=mask_initialized_acute,
        )

        self.population_view.update(pd.concat([scheduled_dates], axis=1))

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit. In the event that a simulant goes for an emergency or
        background visit but already has a followup visit scheduled for the future,
        keep that scheduled followup and do not schedule another one.
        """
        df = self.population_view.get(event.index, query='alive == "alive"')

        # Emergency visits
        mask_acute_is = (
            df[models.ISCHEMIC_STROKE_MODEL_NAME] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi

        # Scheduled visits
        mask_scheduled = (
            df[self.scheduled_visit_date_column] > (event.time - event.step_size)
        ) & (df[self.scheduled_visit_date_column] <= event.time)

        # Background visits
        breakpoint()
        idx_maybe_background_visit = df[~(mask_emergency | mask_scheduled)].index
        utilization_rate = self.background_utilization_rate(idx_maybe_background_visit)
        idx_background_visit = self.randomness.filter_for_rate(
            idx_maybe_background_visit, utilization_rate
        )
        mask_background = pd.Series(False, df.index)
        mask_background.loc[idx_background_visit] = True

        breakpoint()
        df[self.scheduled_visit_date_column] = self.visit_doctor(
            scheduled_dates=df[self.scheduled_visit_date_column],
            event_time=event.time,
            mask=(mask_emergency | mask_scheduled | mask_background),
        )

        self.population_view.update(df[[self.scheduled_visit_date_column]])

    def visit_doctor(
        self,
        scheduled_dates: pd.Series,
        event_time,
        mask: pd.Series,
        min_followup: int = FOLLOWUP_MIN,
        max_followup: int = FOLLOWUP_MAX,
    ) -> pd.Series:
        """Updates treatment plans and schedules followups.

        Arguments:
            scheduled_dates: Series of scheduled dates
            mask: mask of relevant simulants needing followups scheduled
            event_time: the date to check against
            min_followup: minimum number of days out to schedule followup
            max_followup: maximum number of days out to schedule followup
        """
        self.update_treatment()
        return self.schedule_followup(
            scheduled_dates, event_time, mask, min_followup, max_followup
        )

    def update_treatment(self):
        # TODO [MIC-3371, MIC-3375]
        pass

    def schedule_followup(
        self,
        scheduled_dates: pd.Series,
        event_time,
        mask: pd.Series,
        min_followup: int,
        max_followup: int,
    ) -> pd.Series:
        """Schedules follow up visits."""
        idx = scheduled_dates.index
        breakpoint()
        scheduled_dates.loc[mask & ~(scheduled_dates > event_time)] = pd.Series(
            event_time
            + self.random_time_delta(
                pd.Series(min_followup, index=idx),
                pd.Series(max_followup, index=idx),
            ),
            index=idx,
        )
        breakpoint()
        return scheduled_dates

    def random_time_delta(self, start: pd.Series, end: pd.Series) -> pd.Series:
        """Generate a random time delta for each individual in the start
        and end series."""
        return pd.to_timedelta(
            start + (end - start) * self.randomness.get_draw(start.index), unit="day"
        )
