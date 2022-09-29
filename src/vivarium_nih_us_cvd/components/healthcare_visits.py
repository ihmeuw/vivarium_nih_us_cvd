import pandas as pd
import scipy
from vivarium.framework.engine import Builder
from vivarium.framework.event import Event
from vivarium.framework.population import SimulantData

from vivarium_nih_us_cvd.constants import data_keys, data_values, models
from vivarium_nih_us_cvd.utilities import get_measurement_error, schedule_followup


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
        self.clock = builder.time.clock()
        self.step_size = builder.time.step_size()
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
            self.scheduled_visit_date_column,
        ]

        columns_required = [
            "age",
            "sex",
            self.ischemic_stroke_state_column,
            self.myocardial_infarction_state_column,
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
        """Initializes the visit type of simulants in an emergency state so
        that they can be sent to the medication ramps.

        Note that scheduled visits are initialized in the Treatment componenet
        due to their reliance on initial/baseline medication coverage.
        """
        event_time = self.clock() + self.step_size()
        df = self.population_view.subview(
            [
                "age",
                "sex",
                self.ischemic_stroke_state_column,
                self.myocardial_infarction_state_column,
                self.sbp_medication_column,
                self.ldlc_medication_column,
            ]
        ).get(pop_data.index)

        # Initialize new columns
        df[self.visit_type_column] = data_values.VISIT_TYPE.NONE
        df[self.scheduled_visit_date_column] = pd.NaT

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

        # Schedule followups
        # On medication and/or Post/chronic state 0-6 months out
        on_medication = df[
            (df[self.sbp_medication_column].notna())
            | (df[self.ldlc_medication_column].notna())
        ].index
        mask_chronic_is = (
            df[self.ischemic_stroke_state_column] == models.CHRONIC_ISCHEMIC_STROKE_STATE_NAME
        )
        mask_post_mi = (
            df[self.myocardial_infarction_state_column]
            == models.POST_MYOCARDIAL_INFARCTION_STATE_NAME
        )
        acute_history = df.index[mask_chronic_is | mask_post_mi]
        df.loc[
            on_medication.union(acute_history), self.scheduled_visit_date_column
        ] = schedule_followup(
            index=acute_history,
            event_time=event_time,
            randomness=self.randomness,
            min_followup=0,
            max_followup=(data_values.FOLLOWUP_MAX - data_values.FOLLOWUP_MIN),
        )
        # Emergency (acute) state 3-6 months out
        df.loc[emergency, self.scheduled_visit_date_column] = schedule_followup(
            index=emergency,
            event_time=event_time,
            randomness=self.randomness,
        )

        self.population_view.update(
            df[[self.visit_type_column, self.scheduled_visit_date_column]]
        )

    def on_time_step_cleanup(self, event: Event) -> None:
        """Determine if someone will go for an emergency visit, background visit,
        or followup visit and schedule followups. In the event that a simulant
        already has a followup visit scheduled for the future, keep that scheduled
        followup and do not schedule a new one or another one.
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

        # Take measurements (required to determine if a followup is required)
        all_visitors = visit_emergency.union(visit_scheduled).union(visit_background)
        df.loc[all_visitors, "measured_sbp"] = self.sbp(all_visitors) + get_measurement_error(
            index=all_visitors,
            mean=data_values.MEASUREMENT_ERROR_MEAN_SBP,
            sd=data_values.MEASUREMENT_ERROR_SD_SBP,
            randomness=self.randomness,
        )

        # Schedule followups
        visitors_high_sbp = all_visitors.intersection(
            df[df["measured_sbp"] >= data_values.SBP_THRESHOLD.LOW].index
        )
        visitors_on_sbp_medication = all_visitors.intersection(
            df[df[self.sbp_medication_column].notna()].index
        )
        visitors_not_on_sbp_medication = all_visitors.difference(visitors_on_sbp_medication)
        # Schedule those on sbp medication or those not on sbp medication but have a high sbp
        needs_followup = visitors_on_sbp_medication.union(
            visitors_not_on_sbp_medication.intersection(visitors_high_sbp)
        )
        # Do no re-schedule followups that already exist
        has_followup_already_scheduled = df[
            (df[self.scheduled_visit_date_column] > event.time)
        ].index
        to_schedule_followup = needs_followup.difference(has_followup_already_scheduled)
        df.loc[to_schedule_followup, self.scheduled_visit_date_column] = schedule_followup(
            index=to_schedule_followup, event_time=event.time, randomness=self.randomness
        )

        self.population_view.update(
            df[
                [
                    self.visit_type_column,
                    self.scheduled_visit_date_column,
                ]
            ]
        )
