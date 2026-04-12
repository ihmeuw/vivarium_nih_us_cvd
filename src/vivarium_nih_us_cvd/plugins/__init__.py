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
# Monkey-patch ValuesManager.get_value to support attribute pipelines.
#
# In vivarium 4 the values system has two flavors of pipeline: classic
# value pipelines (registered via ``register_value_producer``) and
# attribute pipelines (registered via ``register_attribute_producer``).
# vph 5's ``Risk`` and this project's custom ``DropValueRisk`` /
# ``AdjustedRisk`` / ``TruncatedRisk`` / ``CorrelatedRisk`` classes all
# register ``<risk>.exposure`` as an *attribute* pipeline, while plenty
# of downstream code (``Treatment``, ``HealthcareUtilization``,
# ``CategoricalSBPRisk``) still calls
# ``builder.value.get_value("...exposure")`` and expects a callable
# back. The stock ``get_value`` raises ``DynamicValueError`` once the
# name has been registered as an attribute.
#
# Both pipeline classes are callable with the same signature
# (``pipeline(index)``), so the simplest fix is to make ``get_value``
# return the existing ``AttributePipeline`` when one is registered for
# the requested name. The override only kicks in if the name has been
# registered as an attribute; ordinary value-pipeline lookups go through
# the original code path unchanged. (Component-setup ordering still
# matters: callers that need the attribute version must run after the
# attribute producer is registered. ``Treatment`` and
# ``HealthcareUtilization`` defer their pipeline lookups to a
# ``post_setup`` listener for that reason.)
# ---------------------------------------------------------------------------
import vivarium.framework.values.manager as _vm

_original_values_manager_get_value = _vm.ValuesManager.get_value


def _patched_get_value(self, name):
    if name in self._attribute_pipelines:
        return self._attribute_pipelines[name]
    return _original_values_manager_get_value(self, name)


_vm.ValuesManager.get_value = _patched_get_value

# ---------------------------------------------------------------------------
# Monkey-patch ValuesManager.get_attribute to handle value/attribute pipeline
# ordering conflicts.
#
# In vivarium 4, ``register_value_modifier("X", ...)`` auto-creates a *value*
# pipeline for "X" via ``get_value``.  If ``Risk`` later calls
# ``register_attribute_producer("X", ...)``, ``get_attribute`` raises because
# the name already exists as a value pipeline.  This happens when
# ``InterventionAdherenceEffect`` registers modifiers on medication-adherence
# pipelines *before* ``Risk("risk_factor.sbp_medication_adherence")`` runs.
#
# The fix: when ``get_attribute`` finds the name in ``_value_pipelines``,
# migrate any registered modifiers from the value pipeline to a new attribute
# pipeline, delete the value-pipeline entry, and return the attribute pipeline.
# ---------------------------------------------------------------------------
from vivarium.framework.values.pipeline import AttributePipeline as _AttributePipeline

_original_get_attribute = _vm.ValuesManager.get_attribute


def _patched_get_attribute(self, name):
    if name in self._value_pipelines:
        # Migrate: create attribute pipeline, move modifiers over
        old_pipeline = self._value_pipelines.pop(name)
        attr_pipeline = self._attribute_pipelines.get(name, _AttributePipeline(name))
        self._attribute_pipelines[name] = attr_pipeline
        # Transfer any mutators already registered on the value pipeline.
        for mutator in old_pipeline.mutators:
            mutator._pipeline = attr_pipeline
            attr_pipeline.mutators.append(mutator)
        return attr_pipeline
    return _original_get_attribute(self, name)


_vm.ValuesManager.get_attribute = _patched_get_attribute

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

# ---------------------------------------------------------------------------
# Suppress noisy vivarium 4 warnings that fire on every time step.
#
# 1. PopulationManager warns "attribute pipeline returned a pd.Series with a
#    different name 'value'" every time a LookupTable-backed PAF modifier is
#    evaluated.  This is a vivarium 4 / vph 5 issue: LookupTable returns
#    Series with name='value' rather than the pipeline name.  The framework
#    corrects it automatically, so the warning is harmless.
#
# 2. LookupTableManager warns "configured, but didn't build lookup table"
#    once per component on post-setup.  Our custom risk/effect components
#    handle data loading differently.  Harmless.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Suppress noisy vivarium 4 warnings that fire on every time step.
#
# vivarium uses loguru bound loggers.  We install a global filter on the
# loguru logger that drops known-harmless warning messages.
# ---------------------------------------------------------------------------
import loguru as _loguru

_SUPPRESS_PATTERNS = (
    "returned a pd.Series with a different name",
    "configured, but didn't build lookup table",
    "Conflicting information for",
    "stratifications are registered but not used",
)


def _suppress_known_warnings(record):
    if record["level"].name == "WARNING":
        msg = record["message"]
        for pattern in _SUPPRESS_PATTERNS:
            if pattern in msg:
                return False
    return True


# Remove all existing loguru sinks and re-add with our filter.
# vivarium's LoggingManager checks for sink id 1, so we need to ensure
# the new sink gets that id.  loguru assigns sequential ids: after
# removing 0, the next add gets id 1.
import sys as _sys
_loguru.logger.remove()  # remove all sinks (including default id=0)
_loguru.logger.add(
    _sys.stderr,
    filter=_suppress_known_warnings,
    colorize=True,
    level="WARNING",
)
