from typing import NamedTuple

#############
# Scenarios #
#############


class InterventionScenario:
    def __init__(
        self,
        name: str,
        is_outreach_scenario: bool = False,
        polypill_affects_sbp_adherence: bool = False,
        polypill_affects_sbp_medication: bool = False,
    ):
        self.name = name
        self.is_outreach_scenario = is_outreach_scenario
        self.polypill_affects_sbp_adherence = polypill_affects_sbp_adherence
        self.polypill_affects_sbp_medication = polypill_affects_sbp_medication
        self.is_polypill_scenario = (
            polypill_affects_sbp_adherence or polypill_affects_sbp_medication
        )


class __InterventionScenarios(NamedTuple):
    BASELINE: InterventionScenario = InterventionScenario("baseline")
    OUTREACH_50: InterventionScenario = InterventionScenario(
        "outreach_50",
        is_outreach_scenario=True,
    )
    OUTREACH_100: InterventionScenario = InterventionScenario(
        "outreach_100",
        is_outreach_scenario=True,
    )
    POLYPILL_50: InterventionScenario = InterventionScenario(
        "polypill_50",
        polypill_affects_sbp_adherence=True,
        polypill_affects_sbp_medication=True,
    )
    POLYPILL_100: InterventionScenario = InterventionScenario(
        "polypill_100",
        polypill_affects_sbp_adherence=True,
        polypill_affects_sbp_medication=True,
    )
    POLYPILL_ADHERENCE_ONLY_50: InterventionScenario = InterventionScenario(
        "polypill_adherence_only_50",
        polypill_affects_sbp_adherence=True,
    )
    POLYPILL_ADHERENCE_ONLY_100: InterventionScenario = InterventionScenario(
        "polypill_adherence_only_100",
        polypill_affects_sbp_adherence=True,
    )
    POLYPILL_MEDICATION_ONLY_50: InterventionScenario = InterventionScenario(
        "polypill_medication_only_50",
        polypill_affects_sbp_medication=True,
    )
    POLYPILL_MEDICATION_ONLY_100: InterventionScenario = InterventionScenario(
        "polypill_medication_only_100",
        polypill_affects_sbp_medication=True,
    )
    LIFESTYLE_50: InterventionScenario = InterventionScenario("lifestyle_50")
    LIFESTYLE_100: InterventionScenario = InterventionScenario("lifestyle_100")

    def __getitem__(self, item) -> InterventionScenario:
        for scenario in self:
            if scenario.name == item:
                return scenario
        raise KeyError(item)


INTERVENTION_SCENARIOS = __InterventionScenarios()
