from datetime import datetime
from typing import Dict, Tuple

from vivarium.framework.engine import Builder
from vivarium.framework.time import Time, get_time_stamp
from vivarium_public_health.treatment import LinearScaleUp as LinearScaleUp_


class LinearScaleUp(LinearScaleUp_):
    
    configuration_defaults = {
        "treatment": {
            "date": {
                "start": {
                    "year": "start_year",
                    "month": "start_month",
                    "day": "start_day",
                },
                "end": {
                    "year": "end_year",
                    "month": "end_month",
                    "day": "end_day",
                },
            },
            "value": {
                "start": "data",
                "end": "data",
            },
        }
    }

    def _get_configuration_defaults(self) -> Dict[str, Dict]:
        return {self.configuration_key: LinearScaleUp.configuration_defaults["treatment"]}

    def _get_scale_up_dates(self, builder: Builder) -> Tuple[datetime, datetime]:
        scale_up_config = builder.configuration[self.configuration_key]["date"]

        def get_endpoint(endpoint_type: str) -> datetime:
            if (
                (scale_up_config[endpoint_type]["year"] == f"{endpoint_type}_year")
                & (scale_up_config[endpoint_type]["month"] == f"{endpoint_type}_month")
                & (scale_up_config[endpoint_type]["day"] == f"{endpoint_type}_day")
            ):
                endpoint = get_time_stamp(builder.configuration.time[endpoint_type])
            else:
                endpoint = get_time_stamp(scale_up_config[endpoint_type])
            return endpoint

        return get_endpoint("start"), get_endpoint("end")
