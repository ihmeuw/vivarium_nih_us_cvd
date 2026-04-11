from vivarium_nih_us_cvd.plugins.causes_parser import CausesConfigurationParser

# ---------------------------------------------------------------------------
# Monkey-patch ArtifactManager.load to drop extra draw columns.
#
# vivarium 4.0.1's ArtifactManager.load() renames the first draw column
# (e.g. draw_0) to "value" but leaves all other draw columns (draw_1 …
# draw_249) in the DataFrame.  Downstream, LookupTable._get_columns()
# treats those leftover columns as *categorical key columns*, which
# fragments the data into thousands of tiny groups and causes
# "non-continuous bins" errors in the interpolation layer.
#
# Setting ``input_draw_number`` in the model spec is not viable either:
# the Artifact passes the resulting column list to pd.read_hdf(columns=…),
# which strips ALL columns from non-draw tables (age_bins, etc.).
#
# The fix: after the standard load logic (which renames draw_0 → value),
# drop every remaining draw_* column.
# ---------------------------------------------------------------------------
import vivarium.framework.artifact.manager as _am

_original_artifact_manager_load = _am.ArtifactManager.load


def _patched_load(self, entity_key, **column_filters):
    data = _original_artifact_manager_load(self, entity_key, **column_filters)
    try:
        extra_draws = [c for c in data.columns if str(c).startswith("draw_")]
        if extra_draws:
            data = data.drop(columns=extra_draws)
    except AttributeError:
        pass  # data is not a DataFrame (e.g. metadata dict)
    return data


_am.ArtifactManager.load = _patched_load

# ---------------------------------------------------------------------------
# Monkey-patch risk_distributions.EnsembleDistribution to add
# get_expected_parameters(), which is required by vph 5.0.0 but missing
# from risk_distributions 2.1.3.
#
# Each distribution type has specific parameter names (e.g. norm has
# ['loc', 'scale', 'x_min', 'x_max']).  get_expected_parameters(dist_name)
# returns the list of column names for that distribution.
# ---------------------------------------------------------------------------
import risk_distributions as _rd

_EXPECTED_PARAMETERS = {
    "betasr": ["a", "b", "scale", "loc", "x_min", "x_max"],
    "exp": ["scale", "x_min", "x_max"],
    "gamma": ["a", "scale", "x_min", "x_max"],
    "gumbel": ["loc", "scale", "x_min", "x_max"],
    "invgamma": ["a", "scale", "x_min", "x_max"],
    "invweibull": ["c", "scale", "x_min", "x_max"],
    "llogis": ["c", "d", "scale", "x_min", "x_max"],
    "lnorm": ["s", "scale", "x_min", "x_max"],
    "mgamma": ["a", "scale", "x_min", "x_max"],
    "mgumbel": ["loc", "scale", "x_min", "x_max"],
    "norm": ["loc", "scale", "x_min", "x_max"],
    "weibull": ["c", "scale", "x_min", "x_max"],
}


@classmethod
def _get_expected_parameters(cls, distribution_name: str) -> list:
    return _EXPECTED_PARAMETERS[distribution_name]


if not hasattr(_rd.EnsembleDistribution, "get_expected_parameters"):
    _rd.EnsembleDistribution.get_expected_parameters = _get_expected_parameters
