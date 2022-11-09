from typing import Dict

from vivarium_public_health.treatment import LinearScaleUp

from vivarium_nih_us_cvd.constants import data_values


class OutreachInterventionScaleUp(LinearScaleUp):

    # configuration_defaults = {
    #     "treatment": {
    #         "date": {
    #             "start": {
    #                 "year": data_values.SCALEUP_VALUES.OUTREACH_50.START_YEAR,
    #                 "month": data_values.SCALEUP_VALUES.OUTREACH_50.START_MONTH,
    #                 "day": data_values.SCALEUP_VALUES.OUTREACH_50.START_DAY,
    #             },
    #             "end": {
    #                 "year": data_values.SCALEUP_VALUES.OUTREACH_50.END_YEAR,
    #                 "month": data_values.SCALEUP_VALUES.OUTREACH_50.END_MONTH,
    #                 "day": data_values.SCALEUP_VALUES.OUTREACH_50.END_DAY,
    #             },
    #         },
    #         "value": {
    #             "start": data_values.SCALEUP_VALUES.OUTREACH_50.START_VALUE,
    #             "end": data_values.SCALEUP_VALUES.OUTREACH_50.END_VALUE,
    #         },
    #     },
    # }

    def __init__(self):
        super().__init__("risk_factor.outreach")

    # def _get_configuration_defaults(self) -> Dict[str, Dict]:
    #     return {
    #         self.configuration_key: OutreachInterventionScaleUp.configuration_defaults[
    #             "treatment"
    #         ]
    #     }
