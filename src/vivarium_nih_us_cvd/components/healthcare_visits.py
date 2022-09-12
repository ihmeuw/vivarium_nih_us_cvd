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
        self.visit_type_column = VISIT_TYPE.name
        self.scheduled_visit_date_column = VISIT_TYPE.SCHEDULED_COLUMN_NAME
        columns_created = [self.visit_type_column, self.scheduled_visit_date_column]
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
        builder.event.register_listener("time_step", self.on_time_step)

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """All simulants on SBP medication, LDL-C medication, or a history of
        an acute event (ie intialized in state post-MI or chronic IS) should be
        initialized with a scheduled followup visit 0-6 months out, uniformly
        distributed. All simulants initialized in an acute state should be
        scheduled a followup visit 3-6 months out.

        (a burn-in period will allow the sim to start the observed
        time period with more realistic boundary conditions.)
        """
        step_size = float(self.step_size().days)
        idx = pop_data.index
        visit_types = pd.Series(VISIT_TYPE.NONE, index=idx, name=self.visit_type_column)
        scheduled_dates = pd.Series(pd.NaT, index=idx, name=self.scheduled_visit_date_column)
        states = self.population_view.subview(
            [models.ISCHEMIC_STROKE_MODEL_NAME, models.MYOCARDIAL_INFARCTION_MODEL_NAME]
        ).get(pop_data.index)

        # TODO [MIC-3371, MIC-3375]: Implement medication initialization

        # Schedule appointments for simulants initialized in a post/chronic state
        mask_acute_history = (
            states[models.ISCHEMIC_STROKE_MODEL_NAME]
            == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        ) | (
            states[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        scheduled_dates.loc[mask_acute_history] = pd.Series(
            self.clock()
            + self.random_time_delta(
                pd.Series(step_size, index=idx),
                pd.Series(FOLLOWUP_MAX + 1, index=idx),
            ),
            index=idx,
        )

        # Schedule appointments for simulants initialized in an acute event
        mask_initialized_acute_not_already_scheduled = (scheduled_dates.isna()) & (
            (
                states[models.ISCHEMIC_STROKE_MODEL_NAME]
                == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
            )
            | (
                states[models.MYOCARDIAL_INFARCTION_MODEL_NAME]
                == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
            )
        )

        scheduled_dates.loc[mask_initialized_acute_not_already_scheduled] = pd.Series(
            self.clock()
            + self.random_time_delta(
                pd.Series(FOLLOWUP_MIN, index=idx),
                pd.Series(FOLLOWUP_MAX + 1, index=idx),
            ),
            index=pop_data.index,
        )

        self.population_view.update(pd.concat([visit_types, scheduled_dates], axis=1))

    def on_time_step(self, event: Event) -> None:
        pass

    def on_time_step_cleanup(self, event: Event) -> None:
        pass

    def random_time_delta(self, start: pd.Series, end: pd.Series) -> pd.Series:
        """Generate a random time delta for each individual in the start
        and end series."""
        return pd.to_timedelta(
            start + (end - start) * self.randomness.get_draw(start.index), unit="day"
        )
