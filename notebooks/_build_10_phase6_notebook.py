"""Generate notebooks/10_phase6_easy_observers.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 6
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
# Phase 6: The "Easy" CVD-Local Observers

Phase 6 is Step 1 of the post-Phase-5 migration plan: wire the
three CVD-local observers that were commented out in the
production yaml but whose state-table dependencies are already
present in the Phase 4/5 stack.

Model spec used:
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase6.yaml`.

New components exercised in this phase:

- `HealthcareVisitObserver` — one adding-observation per
  `visit_type` category (`none`, `emergency`, `scheduled`,
  `missed`, `background`), counted at `collect_metrics`.
- `CategoricalColumnObserver` for `sbp_medication`,
  `ldlc_medication`, `outreach`, and `polypill` — one
  person-time observation per level of each column, fired at
  `time_step__prepare`.
- `LifestyleObserver` — special null/non-null split on the
  `lifestyle` column (`cat1` = enrolled, `cat2` = not enrolled).

What this notebook verifies:

1. `InteractiveContext.setup()` succeeds with the new observers
   added — every observer registers its adding-observations via
   the vivarium 4 `requires_attributes` API and uses
   `pop_filter='is_alive == True ...'`.
2. After a short run, the expected result tables show up in
   `sim.get_results()` with sensible totals:
   - Visit counts sum to roughly `pop_size × n_steps`.
   - Medication person-time is dominated by `no_treatment`.
   - Outreach / polypill are entirely in `cat2` under the
     baseline (zero scale-up) scenario.
   - Lifestyle enrollment is tiny but nonzero (driven by
     FPG-test-triggered enrollment inside `HealthcareUtilization`).
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

PHASE6_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase6.yaml'
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up and step

Run setup and six 28-day steps. If this cell raises, the new
observer wiring is broken; if it's quiet, we can move on to
inspecting the result tables.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(PHASE6_SPEC)
print('setup OK')
print('current time:', sim.current_time)
print('population size:', len(sim.get_population(['is_alive'])))

N_STEPS = 6
for _ in range(N_STEPS):
    sim.step()
print(f'{N_STEPS} steps OK, now at {sim.current_time.date()}')
"""
    )
)

cells.append(
    md(
        """\
## 2. Result table inventory

Phase 5 produced 15 result tables. Phase 6 adds:

- 5 `healthcare_visits_*` tables (one per visit type).
- 7 `sbp_medication_*_person_time` + 6 `ldlc_medication_*_person_time`
  tables (one per medication rung).
- 2 `outreach_*_person_time` + 2 `polypill_*_person_time` tables
  (`cat1` / `cat2`).
- 2 `lifestyle_*_person_time` tables.

So we expect 39 tables total. Let's verify and print the new ones.
"""
    )
)

cells.append(
    code(
        """\
results = sim.get_results()
print(f'{len(results)} result tables total')
print()

PHASE5_TABLES = {
    'total_exposure_time_risk_high_ldl_cholesterol_below_2.59',
    'total_exposure_time_risk_high_ldl_cholesterol_between_2.59_and_3.36',
    'total_exposure_time_risk_high_ldl_cholesterol_between_3.36_and_4.14',
    'total_exposure_time_risk_high_ldl_cholesterol_between_4.14_and_4.91',
    'total_exposure_time_risk_high_ldl_cholesterol_above_4.91',
    'total_exposure_time_risk_high_systolic_blood_pressure_below_130.0',
    'total_exposure_time_risk_high_systolic_blood_pressure_between_130.0_and_140.0',
    'total_exposure_time_risk_high_systolic_blood_pressure_above_140.0',
    'deaths', 'ylls', 'ylds',
    'person_time_ischemic_stroke', 'transition_count_ischemic_stroke',
    'person_time_ischemic_heart_disease_and_heart_failure',
    'transition_count_ischemic_heart_disease_and_heart_failure',
}

new_tables = {n: df for n, df in results.items() if n not in PHASE5_TABLES}
print(f'{len(new_tables)} new tables in Phase 6:')
for name, df in new_tables.items():
    total = df['value'].sum()
    print(f'  {name:62s} shape={df.shape} total={total:.2f}')
"""
    )
)

cells.append(
    md(
        """\
## 3. Healthcare visit counts

`HealthcareVisitObserver` counts the number of alive simulants
whose `visit_type` equals each category at the end of every step.
The totals across all 5 visit types should roughly equal
`pop_size × n_steps` (since each simulant has exactly one
`visit_type` per step, including the `none` fallback).

Baseline expectations:

- `none` dominates — most simulants have no visit at all on most
  steps.
- `background` picks up a steady ~15-20% of simulants per step
  (the background-visit ramp in `HealthcareUtilization`).
- `scheduled` / `missed` / `emergency` are small tails.
"""
    )
)

cells.append(
    code(
        """\
visit_totals = {}
for vt in ['none', 'emergency', 'scheduled', 'missed', 'background']:
    df = results[f'healthcare_visits_{vt}']
    visit_totals[vt] = df['value'].sum()

total_visits = sum(visit_totals.values())
expected = 1000 * N_STEPS
print(f'total visit observations: {total_visits:.0f}  (expected ~{expected})')
print()
for vt, n in visit_totals.items():
    share = n / total_visits if total_visits else 0
    print(f'  {vt:12s} {n:7.0f}  ({share:5.1%})')
"""
    )
)

cells.append(
    md(
        """\
## 4. Medication ramp occupancy

`CategoricalColumnObserver` emits person-time (years) spent in
each medication rung. Under baseline we expect most person-time
in `no_treatment`, with a small but nonzero amount on the lower
rungs — the emergency-state bootstrap in the Treatment component
puts some simulants directly onto `one_drug_half_dose_efficacy`
or `two_drug_half_dose_efficacy` at initialization.

Print the sbp_medication and ldlc_medication distributions side
by side.
"""
    )
)

cells.append(
    code(
        """\
def ramp_totals(prefix):
    rows = []
    for name, df in results.items():
        if name.startswith(prefix) and name.endswith('_person_time'):
            rung = name[len(prefix):-len('_person_time')]
            rows.append((rung, df['value'].sum()))
    return pd.DataFrame(rows, columns=['rung', 'person_years']).sort_values('person_years', ascending=False)

sbp = ramp_totals('sbp_medication_')
ldlc = ramp_totals('ldlc_medication_')

print('SBP medication person-time (years):')
print(sbp.to_string(index=False))
print()
print('LDL-C medication person-time (years):')
print(ldlc.to_string(index=False))
print()
print(f'SBP total py : {sbp["person_years"].sum():.2f}')
print(f'LDLC total py: {ldlc["person_years"].sum():.2f}')
"""
    )
)

cells.append(
    md(
        """\
## 5. Intervention exposure (outreach, polypill, lifestyle)

Under the baseline scenario, outreach and polypill exposures are
pinned at 0 and the lifestyle scale-up is flat at 0.0855. We
expect:

- `outreach_cat1_person_time` = 0, `outreach_cat2_person_time` =
  all adult person-time.
- `polypill_cat1_person_time` = 0, `polypill_cat2_person_time` =
  all adult person-time.
- `lifestyle_cat1` (enrolled) picks up small amounts driven by
  FPG-test enrollments inside `HealthcareUtilization`, while
  `lifestyle_cat2` (not enrolled) dominates.
"""
    )
)

cells.append(
    code(
        """\
for name in ['outreach', 'polypill', 'lifestyle']:
    cat1 = results[f'{name}_cat1_person_time']['value'].sum()
    cat2 = results[f'{name}_cat2_person_time']['value'].sum()
    total = cat1 + cat2
    share = cat1 / total if total else 0
    print(f'{name:10s} cat1={cat1:8.2f} py  cat2={cat2:8.2f} py  (cat1 share={share:5.2%})')
"""
    )
)

cells.append(
    md(
        """\
## 6. Spot-check: visit type distribution by age group

A non-trivial check on the new observer output: break down
background visit counts by `age_group`. These totals are count
× simulants × steps, so the widest bin (`5_to_24`, which is 20
years wide vs 5 years for the rest) will naturally dominate.
This is a useful sanity check that the stratifier is stratifying
on the expected set of age groups and that every stratum has
some observed visits.
"""
    )
)

cells.append(
    code(
        """\
bg = results['healthcare_visits_background'].copy()
by_age = (bg.groupby('age_group', observed=True)['value']
              .sum()
              .sort_values(ascending=False))
print('background visit counts by age group (top 10):')
for ag, n in by_age.head(10).items():
    print(f'  {ag:12s} {n:6.0f}')
"""
    )
)

cells.append(
    md(
        """\
## 7. Verdict

If this notebook ran end-to-end without exceptions and:

- `sim.get_results()` returns 39 tables (15 from Phase 5 plus 24
  new ones).
- Healthcare visit counts sum to roughly `pop_size × n_steps`
  with a sensible split across visit types.
- Medication ramp totals are dominated by `no_treatment` but
  include small amounts on the lower rungs.
- Outreach / polypill are 100% `cat2` and lifestyle `cat1` is
  tiny-but-nonzero.
- Background visit counts skew toward older age groups.

then **Phase 6 is complete** and Step 1 of the post-Phase-5
migration plan is done. Step 2 (Phase 7) wires the mediated risk
effects and the PAF calculation simulation, which unblocks
`JointPAFObserver`.
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

out = Path(__file__).parent / "10_phase6_easy_observers.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
