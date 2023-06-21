from typing import Callable, Dict, List

import pandas as pd
from vivarium.framework.state_machine import Transition
from vivarium_public_health.disease import (
    BaseDiseaseState,
    DiseaseState,
    SusceptibleState,
    TransientDiseaseState,
)
from vivarium_public_health.disease.transition import TransitionString

from vivarium_nih_us_cvd.components.causes.transition import CompositeRateTransition


class MultiTransitionState(BaseDiseaseState):
    """A state that has multiple transitions."""

    def __init__(self, cause: str, **kwargs):
        """
        Creates the disease state, a transient state and a rate transition
        between them.
        """
        super().__init__(cause, **kwargs)
        self.transient_state = TransientDiseaseState(f"transient_{self.state_id}")
        self.rate_transition = self.get_rate_transition()
        self._sub_components = self.get_sub_components()

    def set_model(self, model_name: str) -> None:
        super().set_model(model_name)
        self.transient_state.set_model(model_name)

    def get_rate_transition(self) -> CompositeRateTransition:
        """
        Creates a rate transition between the 'real' disease state and its
        associated transient state.
        """
        transition = CompositeRateTransition(self, self.transient_state)
        self.transition_set.append(transition)
        return transition

    def get_sub_components(self) -> List:
        """Sets the state's transition set and transient state as subcomponents"""
        return [self.transition_set, self.transient_state]

    ##################
    # Public methods #
    ##################

    def get_transition_names(self) -> List[str]:
        transitions = []
        for trans in self.transient_state.transition_set.transitions:
            _, _, _, _, end_state = trans.name.split(".")
            transitions.append(TransitionString(f"{self.state_id}_TO_{end_state}"))
        return transitions

    def add_transition(
        self,
        output: BaseDiseaseState,
        source_data_type: str = None,
        get_data_functions: Dict[str, Callable] = None,
        **kwargs,
    ) -> Transition:
        """
        Adds a transition from the state to the output state. The transition
        must be a rate transition.
        """
        if source_data_type == "rate":
            # Adds the rate to the output state to the CompositeRateTransition
            self.rate_transition.add_transition(output.state_id, get_data_functions)

            def get_probability(index: pd.Index) -> pd.Series:
                """
                Computes the probability of transitioning to the output state
                given that a simulant has already transitioned to the transient
                state.

                p = rate_input_to_output / rate_input_to_transient
                """
                rate_to_state = self.rate_transition.get_rate_to_state(index, output)
                total_transition_rate = self.rate_transition.get_transition_rate(index)
                return rate_to_state / total_transition_rate

            transition = Transition(
                self.transient_state, output, probability_func=get_probability
            )
            self.transient_state.transition_set.append(transition)

        else:
            raise ValueError("Only rate transitions are supported.")

        return self.rate_transition


class MultiTransitionSusceptibleState(MultiTransitionState, SusceptibleState):
    pass


class MultiTransitionDiseaseState(MultiTransitionState, DiseaseState):
    pass
