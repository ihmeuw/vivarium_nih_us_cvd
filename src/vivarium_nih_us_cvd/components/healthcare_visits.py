from typing import Dict, Tuple

import numpy as np
import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, data_values, models


class HealthcareVisits:
    """Manages healthcare utilization and scheduling of appointments."""

    configuration_defaults = {}

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self):
        pass

    def __repr__(self):
        return "HealthcareVisits"

    def setup(self, builder: Builder) -> None:
        self.randomness = builder.randomness.get_stream(self.name)
        self.sbp = builder.value.get_value("high_systolic_blood_pressure.exposure")
        self.ldlc = builder.value.get_value("high_ldl_cholesterol.exposure")

        # Load data
        utilization_data = builder.data.load(data_keys.POPULATION.HEALTHCARE_UTILIZATION)
        background_utilization_rate = builder.lookup.build_table(
            utilization_data, parameter_columns=["age", "year"], key_columns=["sex"]
        )
        self.background_utilization_rate = builder.value.register_rate_producer(
            "utilization_rate", background_utilization_rate, requires_columns=["age", "sex"]
        )

        # Columns
        self.ischemic_stroke_state_column = models.ISCHEMIC_STROKE_MODEL_NAME
        self.myocardial_infarction_state_column = models.MYOCARDIAL_INFARCTION_MODEL_NAME
        self.visit_type_column = data_values.COLUMNS.VISIT_TYPE
        self.scheduled_visit_date_column = data_values.COLUMNS.SCHEDULED_VISIT_DATE
        self.sbp_medication_column = data_values.COLUMNS.SBP_MEDICATION
        self.ldlc_medication_column = data_values.COLUMNS.LDLC_MEDICATION

        columns_created = [
            self.visit_type_column,
        ]

        columns_required = [
            "age",
            "sex",
            self.ischemic_stroke_state_column,
            self.myocardial_infarction_state_column,
            self.scheduled_visit_date_column,
            self.sbp_medication_column,
            self.ldlc_medication_column,
        ]

        self.population_view = builder.population.get_view(columns_required + columns_created)

        values_required = [
            "high_systolic_blood_pressure.exposure",
            "high_ldl_cholesterol.exposure",
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
            priority=data_values.COMPONENT_PRIORITIES.HEALTHCARE_VISITS,
        )

    def on_initialize_simulants(self, pop_data: SimulantData) -> None:
        """All simulants on SBP medication, LDL-C medication, or a history of
        an acute event (ie intialized in state post-MI or chronic IS) should be
        initialized with a scheduled followup visit 0-6 months out, uniformly
        distributed. All simulants initialized in an acute state should be
        scheduled a followup visit 3-6 months out, uniformly distributed.

        For simplicity, do not assign background screenings on initialization.
        """

        df = self.population_view.subview(
            [
                "age",
                "sex",
                self.ischemic_stroke_state_column,
                self.myocardial_infarction_state_column,
            ]
        ).get(pop_data.index)

        # Initialize new columns
        df[self.visit_type_column] = data_values.VISIT_TYPE.NONE

        # Update simulants initialized in an emergency state
        mask_acute_is = (
            df[self.ischemic_stroke_state_column] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[self.myocardial_infarction_state_column]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        emergency = df.index[(mask_acute_is | mask_acute_mi)]
        df.loc[emergency, self.visit_type_column] = data_values.VISIT_TYPE.EMERGENCY

        self.population_view.update(df[[self.visit_type_column,]])

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit and schedule followups. In the event that a simulant goes
        for an emergency or background visit but already has a followup visit
        scheduled for the future, keep that scheduled followup and do not schedule
        another one.
        """
        df = self.population_view.subview(
            [
                self.ischemic_stroke_state_column,
                self.myocardial_infarction_state_column,
                self.visit_type_column,
                self.scheduled_visit_date_column,
                self.sbp_medication_column,
                self.ldlc_medication_column,
            ]
        ).get(event.index, query='alive == "alive"')
        df[self.visit_type_column] = data_values.VISIT_TYPE.NONE

        # Emergency visits
        mask_acute_is = (
            df[self.ischemic_stroke_state_column] == models.ACUTE_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_acute_mi = (
            df[self.myocardial_infarction_state_column]
            == models.ACUTE_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        mask_emergency = mask_acute_is | mask_acute_mi
        visit_emergency = df[mask_emergency].index
        df.loc[visit_emergency, self.visit_type_column] = data_values.VISIT_TYPE.EMERGENCY

        # Missed scheduled (non-emergency) visits (these do not get re-scheduled)
        mask_scheduled_non_emergency = (
            (df[self.scheduled_visit_date_column] > (event.time - event.step_size))
            & (df[self.scheduled_visit_date_column] <= event.time)
            & (~mask_emergency)
        )
        scheduled_non_emergency = df[mask_scheduled_non_emergency].index
        df.loc[scheduled_non_emergency, self.scheduled_visit_date_column] = pd.NaT
        visit_missed = scheduled_non_emergency[
            self.randomness.get_draw(scheduled_non_emergency)
            <= data_values.MISS_SCHEDULED_VISIT_PROBABILITY
        ]
        df.loc[visit_missed, self.visit_type_column] = data_values.VISIT_TYPE.MISSED

        # Scheduled visits
        visit_scheduled = scheduled_non_emergency.difference(visit_missed)
        df.loc[visit_scheduled, self.visit_type_column] = data_values.VISIT_TYPE.SCHEDULED

        # Background visits (for those who did not go for another reason or miss their scheduled visit)
        maybe_background = df.index.difference(
            visit_emergency.union(visit_missed).union(visit_scheduled)
        )
        utilization_rate = self.background_utilization_rate(maybe_background)
        visit_background = self.randomness.filter_for_rate(
            maybe_background, utilization_rate
        )  # pd.Index
        df.loc[visit_background, self.visit_type_column] = data_values.VISIT_TYPE.BACKGROUND

        # Take measurements
        all_visitors = visit_emergency.union(visit_scheduled).union(visit_background)
        df.loc[all_visitors, "measured_sbp"] = self.sbp(
            all_visitors
        ) + self.get_measurement_error(
            index=all_visitors,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
        )

        # Schedule followups
        high_sbp = df[df["measured_sbp"] >= data_values.SBP_THRESHOLD.LOW].index
        visitors_high_sbp = all_visitors.intersection(high_sbp)
        on_sbp_medication = df[df[self.sbp_medication_column].notna()].index
        visitors_on_sbp_medication = all_visitors.intersection(on_sbp_medication)
        visitors_not_on_sbp_medication = all_visitors.difference(visitors_on_sbp_medication)
        needs_followup = visitors_on_sbp_medication.union(
            visitors_not_on_sbp_medication.intersection(visitors_high_sbp)
        )
        # Do no re-schedule followups that already exist
        has_followup_already_scheduled = df[
            (df[self.scheduled_visit_date_column] > event.time)
        ].index
        to_schedule_followup = needs_followup.difference(has_followup_already_scheduled)
        df.loc[
            to_schedule_followup, self.scheduled_visit_date_column
        ] = self.schedule_followup(to_schedule_followup, event.time)

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
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

    def get_measurement_error(self, index, mean, sd):
        """Return measurement error assuming normal distribution"""
        draw = self.randomness.get_draw(index)
        return scipy.stats.norm(loc=mean, scale=sd).ppf(draw)
