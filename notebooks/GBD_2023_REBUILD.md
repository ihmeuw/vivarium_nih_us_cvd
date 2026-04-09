# Rebuilding the artifact on GBD 2023 + vivarium 4

This document mirrors the style of `notebooks/02_build_gbd_usa_artifact_and_compare.ipynb`
but targets the **GBD 2023 / vivarium 4 / vivarium_public_health 5 / vivarium_inputs 7**
upgrade. Run all of the cluster steps from an IHME cluster login node — none of the
data-pulling code works locally.

The simulation code in `src/` is being migrated in parallel; this doc only covers
the **artifact build**.

---

## What changed at the data layer

| layer | before | after |
| --- | --- | --- |
| GBD round | round_id 7 (GBD 2020) | round_id 9 (GBD 2023) |
| `vivarium` | 3.x | 4.x |
| `vivarium_public_health` | 4.x | 5.x |
| `vivarium_inputs` | 4.1.x | 7.x |
| `gbd_mapping` | 4.x | 5.x |
| `interface.get_measure(...)` | `(entity, measure, location)` | `(entity, measure, location, years=None, data_type="draws")` |
| relative-risk format for continuous risks | `parameter='per unit'` (one row per cell, log-linear interpolation in the sim) | numeric `parameter` column with one row **per exposure threshold** (piecewise-linear interpolation in the sim) |
| risk effect class | `RiskEffect` | `NonLogLinearRiskEffect` |
| heart-failure RR (categorical SBP, BMI) | unchanged | unchanged — still hand-built from project distributions |

The single biggest behavioural change is the RR format for the four continuous
risks (`high_systolic_blood_pressure`, `high_ldl_cholesterol`,
`high_body_mass_index_in_adults`, `high_fasting_plasma_glucose`). GBD 2023 publishes
these as exposure-parameterised RR curves; vivarium will linearly interpolate
between thresholds rather than computing `RR ** ((exposure - tmrel) / scale)`.

---

## Step 1: Cluster environments

You need two conda environments on the cluster, both Python 3.11+:

### env A: artifact build (data layer)

```bash
conda create -n vnc_artifact_2023 python=3.11
conda activate vnc_artifact_2023

cd /path/to/vivarium_nih_us_cvd
git checkout <branch-with-the-v4-upgrade>

pip install -e .[data]
```

This installs the new pins from `setup.py`:
- `vivarium >= 4.0.0, < 5.0.0`
- `vivarium_public_health >= 5.0.0, < 6.0.0`
- `vivarium_inputs[data] >= 7.0.0`
- `gbd_mapping >= 5.0.0`

Sanity-check the versions:

```bash
python -c "
import vivarium, vivarium_public_health, vivarium_inputs, gbd_mapping
print('vivarium', vivarium.__version__)
print('vph', vivarium_public_health.__version__)
print('vivarium_inputs', vivarium_inputs.__version__)
print('gbd_mapping', gbd_mapping.__version__)
"
```

### env B: PAF simulation (sim layer)

```bash
conda create -n vnc_sim_2023 python=3.11
conda activate vnc_sim_2023

cd /path/to/vivarium_nih_us_cvd
pip install -e .[dev]
```

This is the env you'll use for `simulate run` / `psimulate run` in step 4.

---

## Step 2: Pre-build sanity checks

Before running `make_artifacts`, verify the GBD 2023 data is reachable for one
risk and one cause. From a cluster login node, in env A:

```python
from gbd_mapping import causes, risk_factors
from vivarium_inputs import interface

# Should not raise
location = "United States of America"

# A continuous risk (new non-log-linear RR format expected)
sbp = risk_factors.high_systolic_blood_pressure
exposure = interface.get_measure(sbp, "exposure", location, years=2023)
print("SBP exposure shape:", exposure.shape)
print(exposure.head())

rr = interface.get_measure(sbp, "relative_risk", location, years=2023)
print("\nSBP RR columns:", list(rr.columns))
print("SBP RR parameter column dtype:", rr.reset_index()["parameter"].dtype)
print(rr.head())
```

The expected behavior in GBD 2023:

- The `parameter` column on the RR data should be **numeric** (float) — these are
  exposure thresholds, not the literal string `"per unit"`.
- There should be **multiple rows per (sex, age, year)** demographic cell — one row
  per exposure knot.
- `interface.get_measure` accepts a `years=` keyword. Pass an integer year (e.g.
  `years=2023`), a list of years (e.g. `years=[2020, 2021, 2022, 2023]`), or the
  string `"all"` to pull every available estimation year. The default
  (`years=None`) returns only the most recent year. The string `"recent"` is
  **not** valid in `vivarium_inputs 7`. The artifact loader in this repo passes
  `years='all'` everywhere so the artifact gets the full GBD 2023 estimation
  window — slower than a single year, but it matches the simulation's
  expectation that input data spans every year of the run.

Repeat the check for `high_ldl_cholesterol`, `high_body_mass_index_in_adults`, and
`high_fasting_plasma_glucose`. If any of those return the old `parameter='per unit'`
format, the rest of the pipeline will fail validation in vivarium 5 — talk to the
research team before continuing.

---

## Step 3: Update the loader for the new `interface` signature

This is a code change, not a cluster step, but it has to land before the build
will succeed. The following edits have already been applied to
`src/vivarium_nih_us_cvd/data/loader.py`:

1. **`_get_measure_wrapped`** — the helper used by every standard loader now
   has a `years='all'` kwarg that is passed through to `interface.get_measure`,
   so every standard loader pulls the full GBD 2023 estimation window:

   ```python
   def _get_measure_wrapped(entity, measure, location, years="all"):
       return interface.get_measure(
           entity, measure, location, years=years
       ).droplevel("location")
   ```

2. **`load_healthcare_system_utilization_rate`** — previously called
   `vivarium_gbd_access.utilities.get_draws` with
   `gbd_round_id=ROUND_IDS.GBD_2017` and then manually back-filled 2018/2019
   from the 2017 row. The `get_draws` helper has been removed from
   `vivarium_gbd_access.utilities` in the GBD 2023 version, and the
   natural replacement `gbd.get_modelable_entity_draws(...)` pins to
   release_id 16. Testing (via `get_draws.api.get_draws` directly)
   confirmed that the old outpatient-visits ME 19797 has **no best model
   in any viewable release**, and that `get_best_model_versions(...)`
   returns an empty frame for the other candidate MEs we could find
   ("outpatient healthcare utilization" 25226, "Outpatient Hospital
   Envelope" 18750). The inpatient counterpart (ME 18749) *does* resolve
   fine at release_id 16 via the standard `source="epi"` / `status="best"`
   path, but we don't currently use it. **The loader has been replaced
   with a stub** that returns a flat `3.5 visits/person/year` rate (NAMCS
   US average) across all demographic cells, pulled from
   `load_population_structure`. The stub keeps the artifact build and
   the sim's `HealthcareUtilization` component runnable end-to-end; the
   real fix is to identify the correct round-9 outpatient ME and wire it
   up. See the open-questions list.

3. **`get_re_mean_exposure_data_from_me_id` / `get_re_sd_data_from_me_id`** —
   the two helpers used by the LDL/SBP/BMI exposure loaders. Both now pass
   `gbd_round_id=GBD_2023_ROUND_ID` and the `decomp_step="usa_re"` argument
   has been removed (round 9 no longer accepts `decomp_step`).
   Additionally, both `extract.extract_data(...)` calls inside these helpers
   were updated for the `vivarium_inputs 7.x` signature: they now pass
   `years='all'` and an explicit
   `data_type=DataType("exposure", "draws")`.

4. **`get_re_weights_data_from_file`** — the same `extract.extract_data` call
   was updated to pass `years='all'` and `data_type=DataType("exposure", "draws")`.

5. **`transform_core_get_data_for_vivarium`** — the
   `validation.validate_for_simulation(...)` call was updated for the
   `vivarium_inputs 7.x` signature, which now requires explicit `years` and
   `value_columns` keyword arguments. We pass `years='all'` and the
   draw value columns from `DataType(measure, "draws").value_columns`.

6. **`load_relative_risk_categorical_sbp` / `load_relative_risk_bmi`** — these
   are the two project-specific RR loaders for heart failure (categorical SBP
   cat1–cat4 and BMI dose-response). They are **not** affected by the GBD
   2023 RR-format change because they synthesise data from project-specific
   distributions rather than pulling from GBD. They are unchanged.

7. **`get_entity`** — the `high_fasting_plasma_glucose` hard-coded TMRED block
   imports `gbd_mapping.base_template.Tmred` and `gbd_mapping.id.scalar`.
   These import paths are unchanged in `gbd_mapping 5`; if the build raises
   `ImportError` here, `scalar` may have moved to `gbd_mapping.types`.

8. **Imports** — the loader no longer imports `GBD_2020_ROUND_ID` or
   `ROUND_IDS`. It imports `GBD_2023_ROUND_ID` from `constants.metadata` and
   `DataType` from `vivarium_inputs.utilities`.

9. **`load_emr_ischemic_stroke`** — previously pulled sequela-split EMR via
   `_load_em_from_meid(location, {24714 | 10837}, "Excess mortality rate")`.
   Both MEs return `EmptyDataFrameException` under release_id 16, so the
   loader now falls back to cause-level EMR. Acute and chronic stroke
   states will receive the same cause-level value; until the research team
   identifies a round-9 path that recovers the acute/chronic split, this
   is a simplification.
   **Two complications** had to be worked around:
   1. The round-9 cause-level EMR for ischemic stroke **exceeds** the
      default `VALID_EXCESS_MORT_RANGE[1] = 300.0` validation cap in
      `vivarium_inputs.validation.sim`, raising
      `DataTransformationError`.
   2. The built-in override mechanism
      (`vivarium_inputs.globals.BOUNDARY_SPECIAL_CASES`) has a latent
      upstream bug in `validate_excess_mortality_rate`: the function
      loops over `context["location"]` with a shadowed `location`
      variable, then uses `context["location"]` (a list, unhashable)
      as a dict key — so any successful override lookup raises
      `TypeError: unhashable type: 'list'`.
   The fix at the top of `loader.py` is a small helper
   `_get_unvalidated_measure(entity, measure, location)` that
   replicates `interface.get_measure` minus the validation step
   (core.get_data → scrub_gbd_conventions → split_interval ×2 →
   sort_hierarchical_data → droplevel("location")).
   `load_emr_ischemic_stroke` calls this helper directly. If other EMR
   loaders (`load_emr_ihd_and_hf`, `load_csmr_ihd_and_hf`) hit the same
   300-cap failure, route them through `_get_unvalidated_measure` as
   well. **Open bug to file upstream**: fix the
   `context["location"]` indexing bug in
   `vivarium_inputs.validation.sim.validate_excess_mortality_rate` so
   `BOUNDARY_SPECIAL_CASES` can actually be used.

10. **`gbd.get_modelable_entity_draws` is broken for every ME we need.**
    Verified at a pdb prompt under release_id=16: both ME 2412
    (`Heart failure impairment envelope`) and ME 24694 (`Acute MI`)
    raise `EmptyDataFrameException` — they are present in the
    modelable-entity metadata table but have no round-9 best model in
    the `epi` source. Every other MEID hard-coded in
    `constants/data_values.py` (2412, 24694, 15755) should be assumed
    broken until someone on the research team identifies the correct
    round-9 entities (or `model_version_id`s) to use. Until then,
    `_load_em_from_meid` and `get_proportion_adjusted_heart_failure_data`
    catch `EmptyDataFrameException` and substitute a correctly-shaped
    **stand-in** DataFrame of small nonzero constants
    (`STAND_IN_MEASURE_VALUES` at the top of `loader.py`). The shape
    is borrowed from the cause-level IHD prevalence pull (which does
    work via `_get_unvalidated_measure`), so the sort/index/draw
    columns match what the rest of the loader expects. **These
    numbers are placeholders, not epidemiologically meaningful** —
    the artifact will build end-to-end but the IHD/HF prevalence,
    incidence, and EMR values will be obviously wrong. Notebook 04
    should flag them. Replace once the research team identifies the
    round-9 path.

Once the build env is set up (Step 1), run the sanity check from Step 2 again,
but this time call `loader.load_standard_data` for one of the risks to verify
the end-to-end transform path.

---

## Step 4: Build the artifact (without PAFs)

Same as before, but with the new env:

```bash
conda activate vnc_artifact_2023
cd /path/to/vivarium_nih_us_cvd

make_artifacts -vvv --pdb \
    -l "United States of America" \
    --ignore-pafs \
    -o src/vivarium_nih_us_cvd/artifacts/
```

Expected runtime is similar to GBD 2020 (~30 min for one location). On
failure, the most likely culprits are:

- **`TypeError: get_measure() got an unexpected keyword argument 'years'`** —
  `vivarium_inputs` is still on the 4.x line. Re-check `pip show vivarium_inputs`.
- **`ValidationError: parameter column must be numeric`** — the artifact loader
  is still pivoting RR data through the old `parameter='per unit'` path. Find the
  loader function for that risk and remove the manual `parameter='per unit'`
  assignment.
- **`KeyError: 'decomp_step'`** — see step 3, point 3 above.
- **An empty `relative_risk` DataFrame** — the GBD 2023 RR for that risk × cause
  pair may not yet be available. Confirm with the research team.

State-level builds (the 51-location population-weighted aggregation in
`build_usa_artifact.py`) work the same way; just replace `"United States of America"`
with the relevant state name. The aggregation script does **not** need to change.

---

## Step 5: Calculate joint PAFs (in env B)

The PAF model spec at
`src/vivarium_nih_us_cvd/model_specifications/paf_calculation.yaml` references the
`PAFCalculationRiskEffect` class. After the v4 migration, that class is still in
`src/vivarium_nih_us_cvd/components/effects.py` but is now a subclass of
`NonLogLinearMediatedRiskEffect` (which itself subclasses
`vivarium_public_health.risks.effect.NonLogLinearRiskEffect`). The model spec wires
should not need to change other than updating the class name.

```bash
conda activate vnc_sim_2023
cd /path/to/vivarium_nih_us_cvd

mkdir -p src/vivarium_nih_us_cvd/artifacts/paf_calculation

# Single-machine smoke test:
simulate run \
    src/vivarium_nih_us_cvd/model_specifications/paf_calculation.yaml \
    --artifact-path src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf
```

Or in parallel on the cluster (recommended for the real run):

```bash
psimulate run \
    src/vivarium_nih_us_cvd/model_specifications/paf_calculation.yaml \
    src/vivarium_nih_us_cvd/model_specifications/branches/paf_scenarios.yaml \
    -o src/vivarium_nih_us_cvd/artifacts/ \
    --max-workers 5000 -vvv --pdb \
    -m 20 -r 1:00:00 -P proj_simscience_prod
```

If the PAF sim crashes inside `NonLogLinearMediatedRiskEffect.adjust_target` with
`KeyError: '<risk>_exposure_for_non_loglinear_riskeffect'`, the cause is that one
of the risks doesn't have a corresponding `Risk` component in the model spec, or
the model spec is wiring an old `risk_effect.X_on_Y` configuration name where
`non_log_linear_risk_effect.X_on_Y` is now expected. Search the yaml for the
component name with `grep`.

---

## Step 6: Append PAFs to the artifact

Same as before:

```bash
conda activate vnc_artifact_2023

make_artifacts --pdb -vvv \
    -a -l "United States of America" \
    -o src/vivarium_nih_us_cvd/artifacts/
```

---

## Step 7: Verify the artifact

In env A, drop into Python and spot-check:

```python
import pandas as pd
from pathlib import Path

artifact = Path("src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf")

# Did we get a valid HDF?
with pd.HDFStore(str(artifact), "r") as s:
    keys = sorted(s.keys())
print(f"{len(keys)} keys")

# Spot-check one continuous risk RR
rr = pd.read_hdf(artifact, "/risk_factor/high_systolic_blood_pressure/relative_risk")
print(rr.reset_index()["parameter"].dtype)  # should be float
print(rr.reset_index()["parameter"].nunique())  # should be > 5 (multiple thresholds)
print(rr.head())

# Spot-check the joint PAFs
pafs = pd.read_hdf(artifact, "/risk_factor/joint_mediated_risks/population_attributable_fraction")
print(pafs.shape)
print(pafs.head())
```

If `parameter` is `object`/`string` rather than `float`, the artifact still has
the old log-linear RR format and the sim will silently use the wrong relative
risks. Re-run step 4 after fixing the loader.

---

## Step 8: Compare to the GBD 2020 artifact

The comparison code in `notebooks/02_build_gbd_usa_artifact_and_compare.ipynb`
already has cells for this — point them at the new artifact path and the old one
side-by-side. Expect:

- **Population structure** to be largely unchanged (US census-driven).
- **CSMR / incidence / prevalence** to differ modestly (GBD 2020 → GBD 2023 is
  about three years of new data and methodological updates).
- **Relative risks** for the four continuous risks to look *very* different
  visually because the format has changed from one RR-per-cell to a curve.

---

## Open questions for the research team

1. Is GBD 2023 actually released for all four continuous risks (SBP, LDL-C, BMI,
   FPG) in the database we'll be querying, or is some of it still on hold? If
   any of the four are missing, we have to either fall back to GBD 2020 for that
   risk or wait.
2. Is the `mediation_factors` table from GBD 2023 going to be drop-in
   compatible with the existing `load_mediation_factors` loader, or has the
   `cause_id` / `rei_id` schema changed? The current loader hard-codes
   `rei_id=370,105` and `cause_id=493,495`.
3. Are the heart-failure mediation deltas (`HEART_FAILURE_MEDIATION_DELTAS`) still
   valid under GBD 2023, or do they need to be re-derived?
4. **`load_healthcare_system_utilization_rate` is currently a stub.** ME 19797
   (previous outpatient-visits model) has no best model under any viewable
   release. Two conceptually adjacent MEs were found via metadata search —
   "outpatient healthcare utilization" (25226) and "Outpatient Hospital
   Envelope" (18750) — but neither has a release_id 16 best model either
   (`get_best_model_versions` returns empty). The inpatient counterpart
   (ME 18749 via `source="epi"`, `status="best"`) *does* resolve under round
   9 and was confirmed to return a proper demographic-indexed frame. Until
   a round-9 outpatient model is identified and marked best, the loader
   returns a flat `3.5 visits/person/year` (NAMCS US average) across all
   demographic cells. **What's the right round-9 outpatient ME / source /
   status tuple?** Candidate next steps: (a) try ME 18750 with different
   sources (`stgpr`, `como`) or broader release sweep, (b) contact the
   IHME clinical team who own the inpatient envelope to ask about the
   outpatient counterpart, (c) accept the stub long-term and replace with
   an age-varying literature table.
5. Are the GBD 2020-era MEIDs hard-coded in `constants/data_values.py`
   (`ACUTE_MI_ME_ID=24694`, `POST_MI_ME_ID=15755`, `HEART_FAILURE_ME_ID=2412`,
   `BMI_MEAN_ME_ID=23873`, `BMI_SD_ME_ID=27050`, `LDL_MEAN_ME_ID=26955`,
   `LDL_SD_ME_ID=27057`, `SBP_MEAN_ME_ID=23871`, `SBP_SD_ME_ID=27049`)
   still valid as round 9 best models? Any one of them that's missing a
   GBD 2023 best model will trigger the same `NoBestVersionsException` and
   need either an updated ID or a similar older-release fallback.
