# Vivarium 4 / VPH 5 Issues & Workarounds

Issues discovered during the vivarium_nih_us_cvd migration from vivarium 3 / vivarium_public_health 2 to vivarium 4.0.1 / vivarium_public_health 5.0.0 / risk_distributions 2.1.3.

## Bugs

### 1. ArtifactManager.load leaves extra draw columns

**Location**: `vivarium.framework.artifact.manager.ArtifactManager.load`

When an artifact stores multi-draw data (draw_0 through draw_249), `ArtifactManager.load()` renames `draw_0` to `value` but leaves `draw_1` through `draw_249` in the DataFrame. Downstream, `LookupTable._get_columns()` treats these leftover columns as categorical key columns, which fragments the data into thousands of tiny groups and causes "non-continuous bins" errors in the interpolation layer.

Setting `input_draw_number` in the model spec doesn't help: the Artifact passes the resulting column filter to `pd.read_hdf(columns=...)`, which strips ALL columns from non-draw tables (age_bins, etc.).

**Workaround**: Monkey-patch `ArtifactManager.load` to drop `draw_*` columns after the standard load. See `plugins/__init__.py`.

**Suggested fix**: `ArtifactManager.load` should drop all `draw_*` columns after renaming `draw_0` to `value`. Or provide a clean API for single-draw mode that doesn't break non-draw tables.

---

### 2. ValuesManager.get_value raises on attribute pipelines

**Location**: `vivarium.framework.values.manager.ValuesManager.get_value`

vph 5's `Risk` registers exposure pipelines as *attribute pipelines* (via `register_attribute_producer`), but many downstream components call `builder.value.get_value("risk.exposure")` expecting a callable pipeline back. The stock `get_value` raises `DynamicValueError("'name' is already registered as an attribute pipeline")` when the name exists in `_attribute_pipelines`.

Both `Pipeline` and `AttributePipeline` are callable with the same `pipeline(index)` signature, so returning the existing `AttributePipeline` is safe.

**Workaround**: Monkey-patch `get_value` to return the `AttributePipeline` when one is registered. See `plugins/__init__.py`.

**Suggested fix**: `get_value` should return the `AttributePipeline` instead of raising when one exists, or there should be a `get_pipeline(name)` that returns either type.

---

### 3. LookupTable-backed PAF modifiers trigger per-step name warnings

**Location**: `vivarium.framework.population.manager.PopulationManager` (line ~745)

When an attribute pipeline modifier is a `LookupTable`, it returns a `pd.Series` with `name='value'` (the default column name from the lookup). The `PopulationManager` logs a WARNING on every time step for every such pipeline:

```
The 'X.paf' attribute pipeline returned a pd.Series with a different name 'value'.
```

With 20+ PAF pipelines, this produces ~40 warnings per time step, flooding logs.

**Workaround**: Suppress via loguru filter in `plugins/__init__.py`.

**Suggested fix**: `LookupTable.__call__` should set `result.name = pipeline_name`, or the warning should be logged once then suppressed.

---

### 4. risk_distributions.EnsembleDistribution missing get_expected_parameters

**Location**: `risk_distributions 2.1.3`

vph 5.0.0 calls `EnsembleDistribution.get_expected_parameters(dist_name)` but risk_distributions 2.1.3 doesn't define this method.

**Workaround**: Monkey-patch the method onto `EnsembleDistribution`. See `plugins/__init__.py`.

**Suggested fix**: Add `get_expected_parameters` to `risk_distributions` or pin a version that includes it.

---

### 5. Non-loglinear risk effect detection relies on component name prefix

**Location**: `vivarium_public_health.risks.base_risk.Risk.setup`

vph 5's `Risk` class detects whether it needs to create an exposure cache column for non-loglinear risk effects by checking if any registered component's name starts with `"non_log_linear_risk_effect."`. Custom components that subclass `NonLogLinearRiskEffect` but have different name prefixes (e.g., `"paf_calculation_risk_effect."`) are not detected, so the exposure cache column is never created, causing `KeyError` at runtime.

**Workaround**: Override the detection in custom `CorrelatedRisk.setup()` using `isinstance(component, NonLogLinearRiskEffect)` instead of string prefix matching.

**Suggested fix**: Use `isinstance` checks or a registration mechanism instead of string-matching component names.

---

### 6. `columns_required` property removed without deprecation

**Location**: `vivarium.Component`

Vivarium 3 had `Component.columns_required` as a recognized framework property used to build population views. Vivarium 4 removed it entirely (no `columns_required` attribute on `Component`). Components that define `columns_required` silently become dead code — no error, no warning, but the property has no effect on population view creation.

**Suggested fix**: At minimum, warn when a component defines `columns_required` that it's no longer a framework property.

---

## Noisy but Harmless Warnings

### 7. "configured, but didn't build lookup table" on post-setup

**Location**: `vivarium.framework.lookup.manager.LookupTableManager.on_post_setup`

Every component that is configured with data sources but doesn't call `build_table` for all of them generates a warning. This is expected for components that handle data loading differently (custom risk effects, vph 5 Risk subclasses, disease states using custom initialization).

With a typical CVD model, this produces ~50 warnings at setup time.

---

### 8. "Conflicting information for ... Ignoring 'required_resources'"

**Location**: `vivarium.framework.values.manager.ValuesManager`

When a `LookupTable` is registered as a modifier and `required_resources` are also specified, the framework warns about the conflict and ignores the specified resources. This is correct behavior but generates 6 warnings for our 6 PAF pipeline modifiers.

---

### 9. "stratifications are registered but not used"

**Location**: `vivarium.framework.results.context.ResultsContext`

When stratifications are registered (e.g., `event_year`) but no observer uses them, a warning is emitted. This is informational but adds noise during setup.

---

### 10. `register_value_modifier` auto-creates value pipelines that conflict with later attribute pipelines

**Location**: `vivarium.framework.values.manager.ValuesManager`

When `register_value_modifier("X", ...)` is called before `register_attribute_producer("X", ...)`, the modifier call auto-creates `X` as a value pipeline (via `get_value`). When `Risk` later tries to register the same name as an attribute pipeline, `get_attribute` raises `DynamicValueError("'X' is already registered as a value pipeline")`.

This is order-dependent: if the modifier-registering component sets up before the Risk component (common when custom components are in a different yaml namespace section), the conflict occurs. In the baseline scenario (no modifiers), it doesn't manifest.

**Workaround**: Monkey-patch `get_attribute` to migrate existing value-pipeline modifiers to the new attribute pipeline. See `plugins/__init__.py`.

**Suggested fix**: `register_value_modifier` should not auto-create pipelines. Either queue the modifier or raise a clear error. Alternatively, `get_attribute` should handle existing value pipelines gracefully.

---

## Feature Gaps

### 11. No clean single-draw mode for artifacts with multiple draws

The artifact stores 250 draws. There is no clean way to tell vivarium "use draw 0 and ignore the rest" without breaking non-draw tables. The `input_draw_number` config exists but its implementation strips columns from ALL tables, not just draw-bearing ones.

### 12. Attribute pipelines and value pipelines are incompatible namespaces

A name cannot be both a value pipeline and an attribute pipeline. This forces a choice at component-design time, but downstream consumers may not know which type was registered. The `get_value` / `get_attribute` distinction creates coupling between producer and consumer that didn't exist in vivarium 3.

### 13. Population initialization ordering is fragile

The `required_resources` mechanism for ordering initializers works, but diagnosing ordering failures is difficult. When a pipeline chain tries to read a not-yet-initialized column, the error doesn't indicate which initializer should have run first. The `skip_post_processor=True` escape hatch works but is not documented for this use case.
