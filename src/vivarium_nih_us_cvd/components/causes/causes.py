from importlib import import_module
from typing import Any, Callable, Dict, List, Union

import pandas as pd
from vivarium import Component, ConfigTree
from vivarium.framework.engine import Builder
from vivarium.framework.state_machine import Trigger
from vivarium_public_health.disease import (
    BaseDiseaseState,
    DiseaseModel,
    DiseaseState,
    RecoveredState,
    SusceptibleState,
    TransientDiseaseState,
)
from vivarium_public_health.utilities import TargetString

from vivarium_nih_us_cvd.components.causes.state import (
    MultiTransitionDiseaseState,
    MultiTransitionSusceptibleState,
)
from vivarium_nih_us_cvd.constants.paths import CAUSE_RISK_CONFIG


class Causes(Component):
    # todo could modify the component configuration parser plugin

    #####################
    # Lifecycle methods #
    #####################

    def __init__(self):
        super().__init__()
        self._sub_components = self.get_disease_models()

    ##################
    # Helper methods #
    ##################

    def get_disease_models(self) -> List:
        disease_models = []

        full_config = self.get_causes_configuration()
        config = full_config.causes

        for cause_name, cause_config in config.items():
            states: Dict[str, BaseDiseaseState] = {
                state_name: self.get_state(state_name, state_config, cause_name)
                for state_name, state_config in cause_config.states.items()
            }

            for transition_config in cause_config.transitions.values():
                self.add_transition(
                    states[transition_config.source],
                    states[transition_config.sink],
                    transition_config,
                )

            disease_models.append(DiseaseModel(cause_name, states=list(states.values())))

        return disease_models

    def get_causes_configuration(self):
        full_config = ConfigTree(layers=["default", "cause_model_spec"])
        full_config.update(CAUSE_RISK_CONFIG, layer="cause_model_spec")
        self.mark_multi_transition_states(full_config)
        self.add_default_config_layer(full_config)
        full_config.freeze()
        return full_config

    @staticmethod
    def mark_multi_transition_states(full_config: ConfigTree) -> None:
        transition_counts = {
            cause: {state: 0 for state in config.states}
            for cause, config in full_config.causes.items()
        }

        for cause, config in full_config.causes.items():
            for transition in config.transitions.values():
                transition_counts[cause][transition.source] += 1

        for cause, states in transition_counts.items():
            for state, counts in states.items():
                full_config.causes[cause].states[state].update(
                    {"is_multi_transition": counts > 1}
                )

    @staticmethod
    def add_default_config_layer(config: ConfigTree) -> None:
        default_config = {"causes": {}}
        for cause_name, cause_config in config.causes.items():
            default_states_config = {}
            default_transitions_config = {}
            default_config["causes"][cause_name] = {
                "states": default_states_config,
                "transitions": default_transitions_config,
            }

            for state_name, state_config in cause_config.states.items():
                default_states_config[state_name] = {
                    "cause_type": "cause",
                    "is_multi_transition": False,
                    "transient": False,
                    "allow_self_transition": True,
                    "side_effect": None,
                    "cleanup_function": None,
                }

            for transition_name, transition_config in cause_config.transitions.items():
                default_transitions_config[transition_name] = {"triggered": "NOT_TRIGGERED"}

        config.update(default_config, layer="default")

    def get_state(
        self, state_name: str, state_config: ConfigTree, cause_name: str
    ) -> BaseDiseaseState:
        state_id = cause_name if state_name in ["susceptible", "recovered"] else state_name
        state_kwargs = {
            "cause_type": state_config.cause_type,
            "allow_self_transition": state_config.allow_self_transition,
        }
        if state_config.side_effect:
            # todo handle side effects properly
            state_kwargs["side_effect"] = lambda *x: x
        if state_config.cleanup_function:
            # todo handle cleanup functions properly
            state_kwargs["cleanup_function"] = lambda *x: x
        if "get_data_functions" in state_config:
            data_getters_config = state_config.get_data_functions
            state_kwargs["get_data_functions"] = {
                name: self.get_data_getter(name, data_getters_config[name])
                for name in data_getters_config.keys()
            }

        if state_config.transient:
            state_type = TransientDiseaseState
        elif state_config.is_multi_transition and state_name == "susceptible":
            state_type = MultiTransitionSusceptibleState
        elif state_config.is_multi_transition:
            state_type = MultiTransitionDiseaseState
        elif state_name == "susceptible":
            state_type = SusceptibleState
        elif state_name == "recovered":
            state_type = RecoveredState
        else:
            state_type = DiseaseState

        state = state_type(state_id, **state_kwargs)
        return state

    def add_transition(
        self,
        source_state: BaseDiseaseState,
        sink_state: BaseDiseaseState,
        transition_config: ConfigTree,
    ) -> None:
        triggered = Trigger[transition_config.triggered]
        if "get_data_functions" in transition_config:
            data_getters_config = transition_config.get_data_functions
            data_getters = {
                name: self.get_data_getter(name, data_getters_config[name])
                for name in data_getters_config.keys()
            }
        else:
            data_getters = None

        if transition_config.data_type == "rate":
            source_state.add_rate_transition(
                sink_state, get_data_functions=data_getters, triggered=triggered
            )
        elif transition_config.data_type == "proportion":
            source_state.add_proportion_transition(
                sink_state, get_data_functions=data_getters, triggered=triggered
            )
        elif transition_config.data_type == "dwell_time":
            source_state.add_dwell_time_transition(sink_state, triggered=triggered)
        else:
            raise ValueError(
                f"Invalid transition data type '{transition_config.data_type}'"
                f" provided for transition '{transition_config}'."
            )

    @staticmethod
    def get_data_getter(
        name: str, getter: Union[str, float]
    ) -> Callable[[Builder, Any], Any]:
        if isinstance(getter, float):
            return lambda builder, *_: getter

        try:
            timedelta = pd.Timedelta(getter)
            return lambda builder, *_: timedelta
        except ValueError:
            pass

        if "::" in getter:
            module, method = getter.split("::")
            return getattr(import_module(module), method)

        try:
            target_string = TargetString(getter)
            return lambda builder, *_: builder.data.load(target_string)
        except ValueError:
            pass

        raise ValueError(f"Invalid data getter '{getter}' for '{name}'.")
