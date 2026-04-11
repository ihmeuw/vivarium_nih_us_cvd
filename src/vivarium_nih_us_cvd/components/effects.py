from typing import Callable, Optional

import numpy as np
import pandas as pd
from vivarium import Component
from vivarium.framework.engine import Builder
from vivarium.framework.lookup import LookupTable
from vivarium.framework.time import get_time_stamp
from vivarium_public_health.risks.effect import NonLogLinearRiskEffect, RiskEffect

from vivarium_nih_us_cvd.constants import data_keys, data_values, scenarios
from vivarium_nih_us_cvd.constants.scenarios import InterventionScenario


class InterventionAdherenceEffect(Component):
    """A component to model the impact of the intervention risks on medication adherence levels"""

    #################
    # Setup methods #
    #################

    def setup(self, builder: Builder) -> None:
        self.polypill = builder.value.get_value(data_values.PIPELINES.POLYPILL_EXPOSURE)
        self.scenario = self._get_scenario(builder)
        self.clock = builder.time.clock()
        self.simulation_start_time = get_time_stamp(builder.configuration.time.start)
        self.randomness = builder.randomness.get_stream(self.name)
        self._register_target_modifiers(builder)

    def _get_scenario(self, builder: Builder) -> InterventionScenario:
        return scenarios.INTERVENTION_SCENARIOS[builder.configuration.intervention.scenario]

    def _register_target_modifiers(self, builder: Builder) -> None:
        """which target modifiers get registered depends on the scenario"""
        if self.scenario.is_outreach_scenario:
            builder.value.register_value_modifier(
                data_values.PIPELINES.SBP_MEDICATION_ADHERENCE_EXPOSURE,
                modifier=self._outreach_sbp_adherence_modifier,
            )
            builder.value.register_value_modifier(
                data_values.PIPELINES.LDLC_MEDICATION_ADHERENCE_EXPOSURE,
                modifier=self._outreach_ldlc_adherence_modifier,
            )
        if self.scenario.polypill_affects_sbp_adherence:
            builder.value.register_value_modifier(
                "risk_factor.sbp_medication_adherence.exposure_parameters",
                modifier=self._polypill_sbp_adherence_modifier,
                requires_values=[data_values.PIPELINES.POLYPILL_EXPOSURE],
            )

    def _outreach_sbp_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        return self._outreach_adjust_target(index=index, target=target, medication_type="sbp")

    def _outreach_ldlc_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        return self._outreach_adjust_target(
            index=index, target=target, medication_type="ldlc"
        )

    def _outreach_adjust_target(
        self, index: pd.Index, target: pd.Series, medication_type: str
    ) -> pd.Series:
        clock_time = self.clock()
        # Do not adjust target on intialization. We do this because during
        # initialization the Treatment component sets the medication adherence
        # state table columns equal to the medication adherence pipeline values;
        # this modifier implements the outreach intervention treatment effect
        # which we do want to be active upon initialization
        if clock_time >= self.simulation_start_time:  # not initialization
            primary_non_adherent = target[
                target == data_values.MEDICATION_ADHERENCE_TYPE.PRIMARY_NON_ADHERENT
            ].index
            target[primary_non_adherent] = self.randomness.choice(
                primary_non_adherent,
                choices=list(data_values.OUTREACH_EFFECTS[medication_type].keys()),
                p=list(data_values.OUTREACH_EFFECTS[medication_type].values()),
                additional_key=f"outreach_adjust_{medication_type}_medication_adherence_values",
            )

        return target

    def _polypill_sbp_adherence_modifier(
        self, index: pd.Index, target: pd.Series
    ) -> pd.Series:
        polypill = self.polypill(index)
        on_polypill = polypill[polypill == "cat1"].index
        for (
            cat,
            probability,
        ) in data_values.POLYPILL_SBP_MEDICATION_ADHERENCE_COVERAGE.items():
            target.loc[on_polypill, cat] = probability

        return target


MEDIATOR_NAMES = {
    "high_body_mass_index_in_adults": {
        "acute_ischemic_stroke": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "chronic_ischemic_stroke_to_acute_ischemic_stroke": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "acute_myocardial_infarction": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "post_myocardial_infarction_to_acute_myocardial_infarction": [
            "high_systolic_blood_pressure",
            "high_ldl_cholesterol",
            "high_fasting_plasma_glucose",
        ],
        "heart_failure_from_ischemic_heart_disease": [
            "categorical_high_systolic_blood_pressure",
        ],
        "heart_failure_residual": [
            "categorical_high_systolic_blood_pressure",
        ],
    },
    "high_fasting_plasma_glucose": {
        "acute_ischemic_stroke": [
            "high_ldl_cholesterol",
        ],
        "chronic_ischemic_stroke_to_acute_ischemic_stroke": [
            "high_ldl_cholesterol",
        ],
        "acute_myocardial_infarction": [
            "high_ldl_cholesterol",
        ],
        "post_myocardial_infarction_to_acute_myocardial_infarction": [
            "high_ldl_cholesterol",
        ],
    },
}


class RiskEffectWithoutPAF(RiskEffect):
    """A vph 5 ``RiskEffect`` that skips loading the per-risk PAF table.

    This project supplies population-attributable fractions through a
    separate `JointPAF` component (or, in early build-up phases, doesn't
    apply PAFs at all). The artifact therefore does not contain a
    ``risk_factor.<name>.population_attributable_fraction`` key, and
    vph 5's stock ``RiskEffect.setup`` blows up trying to load it.

    The override here keeps everything else about ``RiskEffect`` intact:
    log-linear relative risks are still loaded, the relative-risk
    pipeline is still registered, and the target rate modifier is still
    applied. We just no-op the PAF half.

    The class also drops a single-value ``parameter`` column from the
    relative-risk table when it's present (the GBD 2023 artifact tags
    log-linear RR rows with ``parameter='per unit'``; vph 5 would
    otherwise treat ``parameter`` as a categorical key column and
    fail to bin it).
    """

    def build_paf_lookup_table(self, builder: Builder) -> None:
        # vph 5's `setup` will assign the return value of this method to
        # `self.paf_table`. We never reference that attribute because we
        # also override `register_paf_modifier` to do nothing.
        return None

    def register_paf_modifier(self, builder: Builder) -> None:
        # No PAF modifier — JointPAF handles attribution adjustments
        # at the level of the joint exposure.
        pass

    def build_rr_lookup_table(self, builder: Builder) -> LookupTable:
        # Mirrors vph 5's RiskEffect.build_rr_lookup_table but strips
        # any single-value 'parameter' column from the loaded RR data
        # before handing it to the lookup table builder.
        self._exposure_distribution_type = self.get_distribution_type(builder)
        rr_data = self.load_relative_risk(builder)
        rr_value_cols = None
        if isinstance(rr_data, pd.DataFrame):
            if "parameter" in rr_data.columns and rr_data["parameter"].nunique() == 1:
                rr_data = rr_data.drop(columns=["parameter"])
        if self.is_exposure_categorical:
            rr_data, rr_value_cols = self.process_categorical_data(builder, rr_data)
        return self.build_lookup_table(
            builder, "relative_risk", data_source=rr_data, value_columns=rr_value_cols,
        )


class NonLogLinearRiskEffectWithoutPAF(NonLogLinearRiskEffect):
    """A vph 5 ``NonLogLinearRiskEffect`` that skips loading the per-risk PAF table.

    Same motivation as ``RiskEffectWithoutPAF``: this project supplies
    PAFs through a separate ``JointPAF`` component, and the GBD 2023
    artifact does not contain
    ``risk_factor.<name>.population_attributable_fraction`` keys for
    these effects.

    The override also makes ``load_relative_risk`` resilient to a
    missing ``input_data.input_draw_number`` config key. vivarium 4
    Artifact applies draw filtering at the HDF level, so we don't set
    ``input_draw_number`` in our model specs; the parent class only
    uses it as a seed input for sampling the TMREL.
    """

    def build_paf_lookup_table(self, builder: Builder) -> None:
        return None

    def register_paf_modifier(self, builder: Builder) -> None:
        # No PAF modifier — JointPAF handles attribution adjustments
        # at the level of the joint exposure.
        pass

    def load_relative_risk(self, builder: Builder, configuration=None):
        # Mirrors NonLogLinearRiskEffect.load_relative_risk but tolerates
        # a missing input_draw_number config (we always treat it as 0).
        import scipy.interpolate
        from vivarium_public_health.utilities import EntityString  # noqa: F401

        if configuration is None:
            configuration = self.configuration

        tmred = builder.data.load(f"{self.risk}.tmred")
        if tmred["distribution"] == "uniform":
            try:
                draw = builder.configuration.input_data.input_draw_number or 0
            except AttributeError:
                draw = 0
            rng = np.random.default_rng(
                builder.randomness.get_seed(self.name + str(draw))
            )
            self.tmrel = rng.uniform(tmred["min"], tmred["max"])
        else:
            raise ValueError(
                f"Unsupported TMRED distribution {tmred['distribution']!r} "
                f"for risk {self.risk.name}"
            )

        rr_source = configuration.data_sources.relative_risk
        original_rrs = self.get_filtered_data(builder, rr_source)
        self.validate_rr_data(original_rrs)

        demographic_cols = [
            col
            for col in original_rrs.columns
            if col != "parameter" and col != "value"
        ]

        def get_rr_at_tmrel(rr_data: pd.DataFrame) -> float:
            interpolated = scipy.interpolate.interp1d(
                rr_data["parameter"],
                rr_data["value"],
                kind="linear",
                bounds_error=False,
                fill_value=(
                    rr_data["value"].min(),
                    rr_data["value"].max(),
                ),
            )
            return interpolated(self.tmrel).item()

        rrs_at_tmrel = (
            original_rrs.groupby(demographic_cols)
            .apply(get_rr_at_tmrel)
            .rename("rr_at_tmrel")
        )
        rr_data = original_rrs.merge(rrs_at_tmrel.reset_index())
        rr_data["value"] = rr_data["value"] / rr_data["rr_at_tmrel"]
        rr_data["value"] = np.clip(rr_data["value"], 1.0, np.inf)
        rr_data = rr_data.drop("rr_at_tmrel", axis=1)
        return rr_data


class MediatedRiskEffect(RiskEffect):
    """Applies mediation to risk effects"""

    def build_all_lookup_tables(self, builder: Builder) -> None:
        """Override to skip PAF loading - this model uses joint PAFs instead."""
        try:
            self._exposure_distribution_type = self.get_distribution_type(builder)
        except ValueError:
            # CategoricalSBPRisk is a custom Component, not a standard Risk,
            # so get_distribution_type can't find it. Infer from the risk name.
            if "categorical" in self.risk.name:
                self._exposure_distribution_type = "ordered_polytomous"
            else:
                self._exposure_distribution_type = "normal"
        rr_data = self.get_filtered_data(
            builder, self.configuration.data_sources.relative_risk
        )
        if self.is_exposure_categorical:
            rr_data, rr_value_cols = self.process_categorical_data(builder, rr_data)
            self.lookup_tables["relative_risk"] = builder.lookup.build_table(
                rr_data,
                value_columns=rr_value_cols,
            )
        else:
            if isinstance(rr_data, pd.DataFrame) and "parameter" in rr_data.columns:
                rr_data = rr_data.drop(columns=["parameter"])
            self.lookup_tables["relative_risk"] = builder.lookup.build_table(
                rr_data,
            )

    def setup(self, builder):
        super().setup(builder)
        # Register unadjusted RR pipelines by passing target=1s to the super's target_modifier
        self.is_target_hf = self.target.name.startswith("heart_failure")
        self.unadjusted_rr = builder.value.register_value_producer(
            f"unadjusted_rr_{self.risk.name}_on_{self.target.name}",
            source=lambda idx: self.adjust_target(idx, pd.Series(1.0, index=idx)),
        )
        self.mediators = MEDIATOR_NAMES.get(self.risk.name, {}).get(self.target.name, [])
        self.unadjusted_mediator_rr = {
            mediator: builder.value.get_value(
                f"unadjusted_rr_{mediator}_on_{self.target.name}"
            )
            for mediator in self.mediators
        }
        # Register the mediation target modifier
        self.mediated_target_modifier = self.get_mediated_target_modifier(builder)
        self.register_mediated_target_modifier(builder)

    #################
    # Setup methods #
    #################

    def register_target_modifier(self, builder: Builder) -> None:
        pass

    def register_paf_modifier(self, builder: Builder) -> None:
        pass

    def get_population_attributable_fraction_source(self, builder: Builder) -> LookupTable:
        pass

    def get_mediated_target_modifier(
        self, builder: Builder
    ) -> Callable[[pd.Index, pd.Series], pd.Series]:
        if self.is_target_hf:
            delta_data = builder.data.load(data_keys.MEDIATION.HF_DELTAS)
            deltas = builder.lookup.build_table(delta_data)

            def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
                unadjusted_rr = self.unadjusted_rr(index)
                scaling_factor = pd.Series(1.0, index=index)
                for mediator in self.mediators:
                    unadjusted_mediator_rr = self.unadjusted_mediator_rr[mediator](index)
                    # NOTE: We only adjust the target RR if the mediator RR is not 1 (TMREL).
                    #   Though not strictly required, it will save computation time.
                    #
                    #   We also only adjust the target RR if the risk RR is not 1 (TMREL)
                    #   in order to be consistent with mediation for IHD and stroke.
                    not_tmrel_idx = index[
                        (unadjusted_mediator_rr != 1.0) & (unadjusted_rr != 1.0)
                    ]
                    scaling_factor.loc[not_tmrel_idx] *= unadjusted_mediator_rr.loc[
                        not_tmrel_idx
                    ] ** deltas(not_tmrel_idx)
                return target * unadjusted_rr / scaling_factor

        else:
            mediation_factors = builder.data.load(data_keys.MEDIATION.MEDIATION_FACTORS)
            mediation_factors = mediation_factors.loc[
                (mediation_factors["risk_name"] == self.risk.name)
                & (mediation_factors["affected_entity"] == self.target.name)
            ]

            def adjust_target(index: pd.Index, target: pd.Series) -> pd.Series:
                unadjusted_rr = self.unadjusted_rr(index)
                scaling_factor = pd.Series(1.0, index=index)
                for mediator in self.mediators:
                    unadjusted_mediator_rr = self.unadjusted_mediator_rr[mediator](index)
                    # NOTE: We only adjust the target RR if the mediator RR is not 1 (TMREL)
                    #   to prevent divide-by-0 errors (since log(1) = 0); it also makes sense
                    #   since RR**x = 1 when RR = 1.
                    #
                    #   We also only adjust the target RR if the risk RR is not 1 (TMREL).
                    #   This is not necessarily required since that would result in delta = 0
                    #   and a scaling factor would resolve to 1 always, but it will
                    #   save some computation time.
                    not_tmrel_idx = index[
                        (unadjusted_mediator_rr != 1.0) & (unadjusted_rr != 1.0)
                    ]
                    mf = mediation_factors.loc[
                        mediation_factors["mediator_name"] == mediator, "value"
                    ].values[0]
                    delta_mediator = np.log(
                        mf * (unadjusted_rr.loc[not_tmrel_idx] - 1) + 1
                    ) / np.log(unadjusted_mediator_rr.loc[not_tmrel_idx])
                    scaling_factor.loc[not_tmrel_idx] *= (
                        unadjusted_mediator_rr.loc[not_tmrel_idx] ** delta_mediator
                    )
                return target * unadjusted_rr / scaling_factor

        return adjust_target

    def register_mediated_target_modifier(self, builder: Builder) -> None:
        builder.value.register_value_modifier(
            self.target_pipeline_name,
            modifier=self.mediated_target_modifier,
            requires_values=[f"{self.risk.name}.exposure"],
            requires_columns=["age", "sex"],
        )


class PAFCalculationRiskEffect(MediatedRiskEffect):
    """Risk effect component for calculating PAFs"""

    def get_population_attributable_fraction_source(
        self, builder: Builder
    ) -> Optional[LookupTable]:
        return None

    def register_target_modifier(self, builder: Builder) -> None:
        pass

    def register_paf_modifier(self, builder: Builder) -> None:
        pass
