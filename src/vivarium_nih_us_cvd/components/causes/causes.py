from importlib import import_module
from typing import Any, Callable, Dict, List, Union

import pandas as pd
from vivarium import ConfigTree
from vivarium.framework.engine import Builder
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


class Causes:
    # todo could modify the component configuration parser plugin
    def __init__(self):
        self._sub_components = self.get_disease_models()

    ##############
    # Properties #
    ##############

    @property
    def name(self) -> str:
        return "causes"

    @property
    def sub_components(self) -> List:
        return self._sub_components

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
                if "get_data_functions" in transition_config:
                    data_getters_config = transition_config.get_data_functions
                    data_getters = {
                        name: self.get_data_getter(name, data_getters_config[name])
                        for name in data_getters_config.keys()
                    }
                else:
                    data_getters = None

                states[transition_config.source].add_transition(
                    states[transition_config.sink],
                    source_data_type=transition_config.data_type,
                    get_data_functions=data_getters,
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
                    "allow_self_transitions": True,
                    "side_effect": None,
                    "cleanup_function": None,
                }

        config.update(default_config, layer="default")

    @staticmethod
    def get_state(
        state_name: str, state_config: ConfigTree, cause_name: str
    ) -> BaseDiseaseState:
        state_kwargs = {"cause_type": state_config.cause_type}
        if state_config.side_effect:
            # todo handle side effects properly
            state_kwargs["side_effect"] = lambda *x: x
        if state_config.cleanup_function:
            # todo handle cleanup functions properly
            state_kwargs["cleanup_function"] = lambda *x: x
        if "get_data_functions" in state_config:
            data_getters_config = state_config.get_data_functions
            state_kwargs["get_data_functions"] = {
                name: Causes.get_data_getter(name, data_getters_config[name])
                for name in data_getters_config.keys()
            }

        if state_config.transient:
            return TransientDiseaseState(state_name, **state_kwargs)

        if state_config.is_multi_transition:
            state = (
                MultiTransitionSusceptibleState(cause_name, **state_kwargs)
                if state_name == "susceptible"
                else MultiTransitionDiseaseState(state_name, **state_kwargs)
            )
        else:
            state = {
                "susceptible": SusceptibleState(cause_name, **state_kwargs),
                "recovered": RecoveredState(cause_name, **state_kwargs),
            }.get(state_name, DiseaseState(state_name, **state_kwargs))

        if state_config.allow_self_transitions:
            state.allow_self_transitions()

        return state

    @staticmethod
    def get_data_getter(
        name: str, getter: Union[str, float]
    ) -> Callable[[Any, Builder], Any]:
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
