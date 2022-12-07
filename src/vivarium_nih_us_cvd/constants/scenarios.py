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
        affect_sbp_adherence: bool = False,
        affect_sbp_medication: bool = False,
    ):
        self.name = name
        self.is_outreach_scenario = is_outreach_scenario
        self.is_polypill_scenario = is_polypill_scenario
        self.affect_sbp_adherence = affect_sbp_adherence
        self.affect_sbp_medication = affect_sbp_medication


class __InterventionScenarios(NamedTuple):
    BASELINE: InterventionScenario = InterventionScenario("baseline")
    OUTREACH_50: InterventionScenario = InterventionScenario(
        "outreach_50", is_outreach_scenario=True
    )
    OUTREACH_100: InterventionScenario = InterventionScenario(
        "outreach_100", is_outreach_scenario=True
    )
    POLYPILL_50: InterventionScenario = InterventionScenario(
        "polypill_50",
        is_polypill_scenario=True,
        affect_sbp_adherence=True,
        affect_sbp_medication=True,
    )
    POLYPILL_100: InterventionScenario = InterventionScenario(
        "polypill_100",
        is_polypill_scenario=True,
        affect_sbp_adherence=True,
        affect_sbp_medication=True,
    )
    POLYPILL_ADHERENCE_ONLY_50: InterventionScenario = InterventionScenario(
        "polypill_adherence_only_50", is_polypill_scenario=True, affect_sbp_adherence=True
    )
    POLYPILL_ADHERENCE_ONLY_100: InterventionScenario = InterventionScenario(
        "polypill_adherence_only_100", is_polypill_scenario=True, affect_sbp_adherence=True
    )
    POLYPILL_MEDICATION_ONLY_50: InterventionScenario = InterventionScenario(
        "polypill_medication_only_50", is_polypill_scenario=True, affect_sbp_medication=True
    )
    POLYPILL_MEDICATION_ONLY_100: InterventionScenario = InterventionScenario(
        "polypill_medication_only_100", is_polypill_scenario=True, affect_sbp_medication=True
    )

    def __getitem__(self, item) -> InterventionScenario:
        for scenario in self:
            if scenario.name == item:
                return scenario
        raise KeyError(item)


INTERVENTION_SCENARIOS = __InterventionScenarios()
