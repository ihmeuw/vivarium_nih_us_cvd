from datetime import datetime
from typing import Tuple

from vivarium.framework.engine import Builder
from vivarium.framework.lookup import LookupTable
from vivarium.framework.time import get_time_stamp
from vivarium_public_health.treatment import LinearScaleUp as LinearScaleUp_


class LinearScaleUp(LinearScaleUp_):
    CONFIGURATION_DEFAULTS = {
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

    #################
    # Setup methods #
    #################

    def get_scale_up_dates(self, builder: Builder) -> Tuple[datetime, datetime]:
        scale_up_config = builder.configuration[self.configuration_key]["date"]
        endpoints = {}
        for endpoint_type in ["start", "end"]:
            if (
                (scale_up_config[endpoint_type]["year"] == f"{endpoint_type}_year")
                & (scale_up_config[endpoint_type]["month"] == f"{endpoint_type}_month")
                & (scale_up_config[endpoint_type]["day"] == f"{endpoint_type}_day")
            ):
                endpoint = get_time_stamp(builder.configuration.time[endpoint_type])
            else:
                endpoint = get_time_stamp(scale_up_config[endpoint_type])
            endpoints[endpoint_type] = endpoint

        return endpoints["start"], endpoints["end"]

    # NOTE: Re-defining to test for future vph fix
    def get_scale_up_values(self, builder: Builder) -> Tuple[LookupTable, LookupTable]:
        scale_up_config = builder.configuration[self.configuration_key]["value"]
        endpoints = {}
        for endpoint_type in ["start", "end"]:
            if scale_up_config[endpoint_type] == "data":
                endpoint = self.get_endpoint_value_from_data(builder, endpoint_type)
            else:
                endpoint = builder.lookup.build_table(scale_up_config[endpoint_type])
            endpoints[endpoint_type] = endpoint

        return endpoints["start"], endpoints["end"]
