"""Generate notebooks/09_phase5_observers.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 5
demo notebook. Execute the notebook with nbconvert to populate outputs.
"""
import json
from pathlib import Path


def md(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": src.splitlines(keepends=True),
    }


def code(src: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


cells = []

cells.append(
    md(
        """\
# Phase 5: Full Observer Suite

Phase 5 is the final step in the GBD 2020 → GBD 2023 / vivarium 3 →
vivarium 4 build-up. It takes the complete Phase 4 stack (disease
models + risks + risk effects + healthcare / treatment / interventions)
and wires in the result-producing observers on top.

Model spec used:
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase5.yaml`.

New components exercised in this phase:

- vivarium_public_health 5 `MortalityObserver`, `DisabilityObserver`,
  and two `DiseaseObserver` instances (acute/chronic ischemic stroke
  and the IHD+HF disease model).
- The project's own `BinnedRiskObserver` for
  `risk_factor.high_systolic_blood_pressure` and
  `risk_factor.high_ldl_cholesterol`, which buckets
  exposure-time by clinical thresholds.
- The `SimpleResultsStratifier` / `ResultsStratifier` hierarchy,
  which patches the GBD 2023 age_bins table (drops the stray
  `index` column and redefines the youngest bin as `5_to_24`).

What this notebook verifies:

1. `InteractiveContext.setup()` succeeds — observers register
   adding-observations via the vivarium 4
   `register_adding_observation(..., requires_attributes=...)`
   API with `pop_filter='is_alive == True'`.
2. The stratifier produces the expected age bins for the GBD 2023
   artifact (no leftover `index` column, youngest bin merged into
   `5_to_24`, top bin extended so simulants cannot age past it
   within a single step).
3. Stepping the simulation accumulates observations every
   `time_step__prepare` / `collect_metrics` step. We run a short
   stretch of steps and then read the results via
   `sim.get_results()`.
4. The result tables have sensible shapes and totals — person-time
   adds up across the binned risk observers, deaths / ylls / ylds
   are non-negative, and disease transition counts show some motion.
"""
    )
)

cells.append(
    code(
        """\
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from vivarium import InteractiveContext

plt.rcParams['figure.figsize'] = (10, 4)
plt.rcParams['figure.dpi'] = 100
pd.set_option('display.max_columns', 40)
pd.set_option('display.width', 200)

PHASE5_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase5.yaml'
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up the simulation

If `InteractiveContext(...)` returns without raising, the Phase 5
observer machinery is wired up: each observer has registered its
adding-observations, the stratifier has registered the default
`age_group` / `sex` / `current_year` stratifications plus the
medication-adherence stratifications, and every `requires_attributes`
dependency has a resource producer in place.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(PHASE5_SPEC)
print('setup OK')
print('current time:', sim.current_time)
print('population size:', len(sim.get_population(['is_alive'])))
"""
    )
)

cells.append(
    md(
        """\
## 2. Stratifier sanity check

The `SimpleResultsStratifier.get_age_bins` override is what makes
the Phase 5 results machinery usable against the GBD 2023 USA
artifact. It:

1. Drops the stray `index` column that comes back from
   `reset_index()` on the artifact's MultiIndex age_bins table.
2. Keeps only bins with `age_start >= 25`, then prepends a
   synthetic `5_to_24` bin so we still have coverage for the
   youngest simulants.
3. Extends the top bin by one step so a simulant cannot age past
   it within a single 28-day step (MIC-4083 workaround).

We don't have the builder handy after setup, so we verify the
stratification indirectly after stepping (next section) by reading
back the distinct `age_group` values that appear in the result
tables.
"""
    )
)

cells.append(
    code(
        """\
age_range = sim.get_population(['age'])['age']
print('population adult age range:',
      round(age_range.min(), 2), '->', round(age_range.max(), 2))
print('age distribution summary:')
print(age_range.describe().round(2).to_string())
"""
    )
)

cells.append(
    md(
        """\
## 3. Step the simulation and collect results

Run a handful of 28-day steps. Each step fires
`time_step__prepare` observations (the binned risk observers) and
then `collect_metrics` observations (the vph 5 disease / mortality
/ disability observers). After the run, `sim.get_results()` returns
a dict of result-name → DataFrame, one per registered
adding-observation.
"""
    )
)

cells.append(
    code(
        """\
N_STEPS = 6
for step_i in range(N_STEPS):
    sim.step()
print(f'{N_STEPS} steps OK, now at {sim.current_time.date()}')
"""
    )
)

cells.append(
    code(
        """\
results = sim.get_results()
print(f'{len(results)} result tables registered')
print()
for name, df in results.items():
    print(f'  {name:70s} shape={df.shape}')

# Age-group stratification: pick any vph-observer result and read
# off the distinct age_group categories. We expect the synthetic
# `5_to_24` bin at the bottom and the standard GBD bins above.
sample = results['deaths']
print()
print('distinct age groups seen in deaths table:')
for ag in sorted(sample['age_group'].unique()):
    print(f'  {ag}')
"""
    )
)

cells.append(
    md(
        """\
## 4. Binned risk observer totals

The `BinnedRiskObserver` counts person-time (in years) inside each
exposure-threshold bin, stratified by age / sex / year. Summing
across a risk's bins should give us something close to
`population_size * simulated_time` (minus any time outside the
stratified universe).

For LDL-C we expect bins split at `2.59 / 3.36 / 4.14 / 4.91`
mmol/L. For SBP we expect bins split at `130 / 140` mmHg. The
totals should be on the order of 1000 simulants × ~6/13 of a
year ≈ 460 person-years.
"""
    )
)

cells.append(
    code(
        """\
def total_pt(prefix):
    return sum(
        float(df['value'].sum())
        for name, df in results.items()
        if name.startswith(prefix)
    )

ldlc_pt = total_pt('total_exposure_time_risk_high_ldl_cholesterol')
sbp_pt = total_pt('total_exposure_time_risk_high_systolic_blood_pressure')
print(f'LDL-C total person-time (years): {ldlc_pt:.2f}')
print(f'SBP   total person-time (years): {sbp_pt:.2f}')

print()
print('LDL-C bin split:')
for name, df in results.items():
    if name.startswith('total_exposure_time_risk_high_ldl_cholesterol'):
        print(f'  {name:80s} sum={df["value"].sum():.2f}')

print()
print('SBP bin split:')
for name, df in results.items():
    if name.startswith('total_exposure_time_risk_high_systolic_blood_pressure'):
        print(f'  {name:80s} sum={df["value"].sum():.2f}')
"""
    )
)

cells.append(
    md(
        """\
## 5. Mortality / disability totals

`MortalityObserver` and `DisabilityObserver` produce one row per
cause × stratum. `deaths` counts death events, `ylls` and `ylds`
accumulate years-of-life-lost / years-lived-with-disability. Over
the short simulated window these will be small but strictly
non-negative.
"""
    )
)

cells.append(
    code(
        """\
for key in ['deaths', 'ylls', 'ylds']:
    df = results[key]
    total = df['value'].sum()
    print(f'{key:8s} total={total:10.4f}  rows={len(df)}')
    nonzero = df[df['value'] > 0]
    if len(nonzero):
        top = (nonzero.groupby('sub_entity', observed=True)['value']
                        .sum().sort_values(ascending=False))
        print('  breakdown by cause:')
        for idx, val in top.head(5).items():
            print(f'    {idx:50s} {val:.4f}')
    else:
        print('  (no nonzero rows)')
"""
    )
)

cells.append(
    md(
        """\
## 6. Disease observer totals

`DiseaseObserver` produces two tables per disease model:
`person_time_<model>` (person-time in each disease state) and
`transition_count_<model>` (counts of transitions between states).
A healthy simulation will accumulate most of its person-time in
the susceptible state with some transitions into acute / chronic
states.
"""
    )
)

cells.append(
    code(
        """\
for disease in [
    'ischemic_stroke',
    'ischemic_heart_disease_and_heart_failure',
]:
    pt = results[f'person_time_{disease}']
    tc = results[f'transition_count_{disease}']
    print(f'== {disease} ==')
    print(f'  person-time total (years): {pt["value"].sum():.2f}  rows={len(pt)}')
    pt_by_state = (pt.groupby('sub_entity', observed=True)['value']
                      .sum().sort_values(ascending=False))
    print('  person-time by state:')
    for idx, val in pt_by_state.items():
        print(f'    {idx:60s} {val:.2f}')
    nonzero_tc = tc[tc['value'] > 0]
    if len(nonzero_tc):
        tc_by_trans = (nonzero_tc.groupby('sub_entity', observed=True)['value']
                                  .sum().sort_values(ascending=False))
        print('  observed transitions:')
        for idx, val in tc_by_trans.items():
            print(f'    {idx:60s} {val:.0f}')
    else:
        print('  (no transitions observed in this run)')
    print()
"""
    )
)

cells.append(
    md(
        """\
## 7. Verdict

If this notebook ran end-to-end without exceptions and:

- The stratifier returns an `age_bins` table with a `5_to_24`
  bottom bin and no stray `index` column.
- All 15 result tables (`total_exposure_time_risk_high_ldl_cholesterol_*`
  × 5, `total_exposure_time_risk_high_systolic_blood_pressure_*`
  × 3, `deaths` / `ylls` / `ylds`, and
  `person_time_*` / `transition_count_*` for the two disease
  models) are populated with non-trivial values.
- The binned risk observer totals are within a factor of 2 of the
  expected `population_size × simulated_time` (here ~460 py).
- At least one transition is observed in each disease model over
  six 28-day steps.

then **Phase 5 is complete** and the incremental GBD 2023 /
vivarium 4 build-up plan has reached its final phase. The
remaining non-Phase work (wiring back `HealthcareVisitObserver`,
`CategoricalColumnObserver`, `LifestyleObserver`, and the
`JointPAFObserver` once `NonLogLinearMediatedRiskEffect` is in
place) is tracked separately.
"""
    )
)


nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.11",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).parent / "09_phase5_observers.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
