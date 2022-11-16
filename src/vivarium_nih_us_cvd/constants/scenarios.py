from typing import NamedTuple

#############
# Scenarios #
#############


class InterventionScenario:
    def __init__(
        self,
        name: str,
        is_outreach_scenario: bool = False,
        is_polypill_scenario: bool = False,
    ):
        self.name = name
        self.is_outreach_scenario = is_outreach_scenario
        self.is_polypill_scenario = is_polypill_scenario


class __InterventionScenarios(NamedTuple):
    BASELINE: InterventionScenario = InterventionScenario("baseline")
    OUTREACH_50: InterventionScenario = InterventionScenario(
        "outreach_50", is_outreach_scenario=True
    )
    OUTREACH_100: InterventionScenario = InterventionScenario(
        "outreach_100", is_outreach_scenario=True
    )
    POLYPILL_50: InterventionScenario = InterventionScenario(
        "polypill_50", is_polypill_scenario=True
    )
    POLYPILL_100: InterventionScenario = InterventionScenario(
        "polypill_100", is_polypill_scenario=True
    )

    def __getitem__(self, item) -> InterventionScenario:
        for scenario in self:
            if scenario.name == item:
                return scenario
        raise KeyError(item)


INTERVENTION_SCENARIOS = __InterventionScenarios()
