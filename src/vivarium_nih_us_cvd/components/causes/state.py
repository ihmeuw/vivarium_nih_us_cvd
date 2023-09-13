from typing import Callable, Dict, List, Optional

import pandas as pd
from vivarium import Component
from vivarium.framework.state_machine import Transition
from vivarium_public_health.disease import (
    BaseDiseaseState,
    DiseaseState,
    SusceptibleState,
    TransientDiseaseState,
)
from vivarium_public_health.disease.transition import (
    ProportionTransition,
    RateTransition,
    TransitionString,
)

from vivarium_nih_us_cvd.components.causes.transition import CompositeRateTransition


class MultiTransitionState(BaseDiseaseState):
    """A state that has multiple transitions."""

    ##############
    # Properties #
    ##############

    @property
    def sub_components(self) -> List[Component]:
        return [self.transition_set, self.transient_state]

    #####################
    # Lifecycle methods #
    #####################

    def __init__(
        self,
        state_id: str,
        allow_self_transition: bool = False,
        side_effect_function: Optional[Callable] = None,
        cause_type: str = "cause",
    ):
        """
        Creates the disease state, a transient state and a rate transition
        between them.
        """
        super().__init__(
            state_id,
            allow_self_transition=allow_self_transition,
            side_effect_function=side_effect_function,
            cause_type=cause_type,
        )
        self.transient_state = TransientDiseaseState(f"transient_{self.state_id}")
        self.rate_transition = self.get_rate_transition()

    ##########################
    # Initialization methods #
    ##########################

    def get_rate_transition(self) -> CompositeRateTransition:
        """
        Creates a rate transition between the 'real' disease state and its
        associated transient state.
        """
        transition = CompositeRateTransition(self, self.transient_state)
        self.transition_set.append(transition)
        return transition

    ##################
    # Public methods #
    ##################

    def set_model(self, model_name: str) -> None:
        super().set_model(model_name)
        self.transient_state.set_model(model_name)

    def get_transition_names(self) -> List[str]:
        transitions = []
        for transition in self.transient_state.transition_set.transitions:
            end_state = transition.output_state.name.split(".")[1]
            transitions.append(TransitionString(f"{self.state_id}_TO_{end_state}"))
        return transitions

    def add_rate_transition(
        self,
        output: BaseDiseaseState,
        get_data_functions: Dict[str, Callable] = None,
        **kwargs,
    ) -> RateTransition:
        """Adds a rate transition from the state to the output state."""
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

        self.transient_state.add_transition(transition)
        return self.rate_transition

    def add_proportion_transition(
        self,
        output: BaseDiseaseState,
        get_data_functions: Dict[str, Callable] = None,
        **kwargs,
    ) -> ProportionTransition:
        raise ValueError(f"Only rate transitions are allowed to exit a {type(self)}.")

    def add_dwell_time_transition(
        self,
        output: BaseDiseaseState,
        **kwargs,
    ) -> Transition:
        raise ValueError(f"Only rate transitions are allowed to exit a {type(self)}.")


class MultiTransitionSusceptibleState(MultiTransitionState, SusceptibleState):
    pass


class MultiTransitionDiseaseState(MultiTransitionState, DiseaseState):
    pass
