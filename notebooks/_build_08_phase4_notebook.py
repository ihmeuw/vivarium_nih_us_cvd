"""Generate notebooks/08_phase4_healthcare_treatment.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 4
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
# Phase 4: Healthcare Utilization + Treatment + Interventions

Phase 4 layers the full healthcare-delivery pipeline on top of
Phase 3. The model spec used is
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase4.yaml`.

New components exercised in this phase:

- `HealthcareUtilization` (with `Treatment` as a sub-component) —
  drives emergency / scheduled / background visits, FPG testing,
  lifestyle enrollment, and SBP / LDL-C medication ramps.
- vph 5 `Risk` components for the medication-adherence and
  intervention exposures (`sbp_medication_adherence`,
  `ldlc_medication_adherence`, `outreach`, `polypill`, `lifestyle`).
- `InterventionAdherenceEffect` and three `LinearScaleUp` wrappers
  for outreach / polypill / lifestyle (scale-up schedules are
  all flat under the baseline scenario used here).

What this notebook verifies:

1. `InteractiveContext.setup()` succeeds — all the new vivarium 4
   APIs (`register_attribute_modifier`, `register_value_modifier`,
   `register_rate_producer`, `register_initializer(...)`) line
   up, and every component's `required_resources` gets satisfied
   in topological order.
2. The Treatment-created columns (`sbp_medication`,
   `ldlc_medication`, both adherence columns, the multiplier
   columns, outreach/polypill, therapeutic-inertia components)
   initialize with sensible values.
3. The HealthcareUtilization-created columns (`visit_type`,
   `scheduled_visit_date`, `last_fpg_test_date`, `lifestyle`)
   initialize consistently (some simulants start on the emergency
   ramp, some get back-dated FPG tests).
4. Stepping the simulation fires the visit / treatment ramps: after
   a handful of steps we should see a spread of visit types,
   treatment escalations, and a few lifestyle enrollments.
5. The underlying risk exposures still respond to the medication
   multipliers — `AdjustedRisk.get_current_exposure` produces
   lowered SBP / LDL-C for simulants with non-trivial multipliers.
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

PHASE4_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase4.yaml'
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up the simulation

If `InteractiveContext(...)` returns without raising, all the
Phase 4 machinery is wired up: Treatment has registered the
medication / adherence / multiplier columns via
`register_initializer`, HealthcareUtilization has registered the
visit / test / lifestyle columns, and every risk exposure /
intervention value modifier has a source.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(PHASE4_SPEC)
print('setup OK')
print('current time:', sim.current_time)
"""
    )
)

cells.append(
    md(
        """\
## 2. Initial treatment and visit state

Pull the Treatment + HealthcareUtilization columns and inspect
the baseline distribution. At t=0 (after `initialize_simulants`
but before any step) we should see:

- Most simulants on `no_treatment` for SBP/LDL-C, with a minority
  on the lower rungs of the medication ramp (as seeded by the
  baseline coverage lookups + the emergency-state bootstrap).
- A handful of simulants flagged as `emergency` visit type (those
  initialized in an acute state).
- A subset of simulants with a back-dated `last_fpg_test_date`.
"""
    )
)

cells.append(
    code(
        """\
TREATMENT_COLS = [
    'age', 'sex',
    'sbp_medication', 'ldlc_medication',
    'sbp_medication_adherence', 'ldlc_medication_adherence',
    'sbp_multiplier', 'ldlc_multiplier',
    'outreach', 'polypill',
    'visit_type', 'scheduled_date',
    'last_fpg_test_date', 'lifestyle',
    'ischemic_stroke', 'ischemic_heart_disease_and_heart_failure',
]

pop0 = sim.get_population(TREATMENT_COLS)
print(f'initial pop: {len(pop0)} simulants')
print()
print('sbp_medication value counts:')
print(pop0['sbp_medication'].value_counts().to_string())
print()
print('ldlc_medication value counts:')
print(pop0['ldlc_medication'].value_counts().to_string())
print()
print('visit_type value counts:')
print(pop0['visit_type'].value_counts().to_string())
"""
    )
)

cells.append(
    code(
        """\
print('sbp_multiplier describe:')
print(pop0['sbp_multiplier'].describe().round(4))
print()
print('ldlc_multiplier describe:')
print(pop0['ldlc_multiplier'].describe().round(4))
print()
print('last_fpg_test_date non-null:', pop0['last_fpg_test_date'].notna().sum())
print('lifestyle non-null (enrolled pre-sim):', pop0['lifestyle'].notna().sum())
"""
    )
)

cells.append(
    md(
        """\
## 3. Risk exposures reflect medication multipliers

`AdjustedRisk.get_current_exposure` returns
`gbd_exposure * multiplier_column`. The multiplier represents the
factor that "undoes" the average treatment effect already baked
into the GBD-observed exposure: simulants on medication carry a
multiplier ≥ 1, so the source value of `risk.exposure` sits
*above* `risk.gbd_exposure`. Untreated simulants have multiplier
= 1.0, leaving the two equal. (The published `risk.exposure`
attribute pipeline then subtracts a `drop_value` for any polypill
/ intervention effects, so for LDL-C the post-processed
distribution can come in *below* the raw GBD exposure even where
the underlying multiplier is > 1.)
"""
    )
)

cells.append(
    code(
        """\
EXPOSURE_ATTRS = [
    'high_systolic_blood_pressure.exposure',
    'high_ldl_cholesterol.exposure',
    'high_body_mass_index_in_adults.exposure',
    'high_fasting_plasma_glucose.exposure',
    'sbp_multiplier', 'ldlc_multiplier',
    'age',
]
pop_exp = sim.get_population(EXPOSURE_ATTRS)
# gbd_exposure is a value pipeline, not an attribute, so fetch via get_value
sbp_gbd = sim.get_value('high_systolic_blood_pressure.gbd_exposure')(pop_exp.index)
ldlc_gbd = sim.get_value('high_ldl_cholesterol.gbd_exposure')(pop_exp.index)
pop_exp['sbp_gbd_exposure'] = sbp_gbd
pop_exp['ldlc_gbd_exposure'] = ldlc_gbd

adults = pop_exp[pop_exp['age'] >= 25].copy()
print(f'{len(adults)} adults')
print()
print('SBP: gbd vs observed exposure (adults):')
print(adults[[
    'sbp_gbd_exposure',
    'high_systolic_blood_pressure.exposure',
]].describe().round(2))
print()
print('LDL-C: gbd vs observed exposure (adults):')
print(adults[[
    'ldlc_gbd_exposure',
    'high_ldl_cholesterol.exposure',
]].describe().round(2))
"""
    )
)

cells.append(
    code(
        """\
# Treated simulants carry multiplier > 1; untreated == 1.
# `risk.exposure` should sit above `risk.gbd_exposure` for the treated.
treated_sbp = adults[adults['sbp_multiplier'] > 1.0]
print(f'{len(treated_sbp)} adults with sbp_multiplier > 1')
if len(treated_sbp):
    bump = (treated_sbp['high_systolic_blood_pressure.exposure']
            - treated_sbp['sbp_gbd_exposure'])
    print('SBP untreated-vs-GBD gap for treated (mmHg):')
    print(bump.describe().round(3))

treated_ldlc = adults[adults['ldlc_multiplier'] > 1.0]
print()
print(f'{len(treated_ldlc)} adults with ldlc_multiplier > 1')
if len(treated_ldlc):
    bump = (treated_ldlc['high_ldl_cholesterol.exposure']
            - treated_ldlc['ldlc_gbd_exposure'])
    print('LDL-C untreated-vs-GBD gap for treated (mmol/L):')
    print(bump.describe().round(3))
"""
    )
)

cells.append(
    md(
        """\
## 4. Step the simulation forward

Run a handful of 28-day steps and watch the visit / treatment
state evolve. After a few months we should see:

- A nontrivial fraction of simulants flagged with `background` or
  `scheduled` visit types on any given step.
- Medication escalation: more simulants on non-`no_treatment`
  rungs, especially on the lower SBP rungs.
- A growing number of simulants with `lifestyle` enrollment
  timestamps (under the baseline 0% scale-up this should stay
  small but nonzero since testing can still pick simulants up).
- Disease state transitions consistent with Phase 3 (some new
  cases of acute / post-MI, AIS).
"""
    )
)

cells.append(
    code(
        """\
history = []
for step_i in range(6):
    sim.step()
    snap = sim.get_population(TREATMENT_COLS)
    history.append({
        'step': step_i + 1,
        'time': str(sim.current_time.date()),
        'any_treatment_sbp': int((snap['sbp_medication'] != 'no_treatment').sum()),
        'any_treatment_ldlc': int((snap['ldlc_medication'] != 'no_treatment').sum()),
        'visit_emergency': int((snap['visit_type'] == 'emergency').sum()),
        'visit_scheduled': int((snap['visit_type'] == 'scheduled').sum()),
        'visit_background': int((snap['visit_type'] == 'background').sum()),
        'lifestyle_enrolled': int(snap['lifestyle'].notna().sum()),
        'acute_is': int((snap['ischemic_stroke'] == 'acute_ischemic_stroke').sum()),
        'acute_mi': int((snap['ischemic_heart_disease_and_heart_failure']
                          == 'acute_myocardial_infarction').sum()),
    })

history_df = pd.DataFrame(history).set_index('step')
print(history_df)
"""
    )
)

cells.append(
    code(
        """\
pop_after = sim.get_population(TREATMENT_COLS)
print('after 6 steps:')
print()
print('sbp_medication value counts:')
print(pop_after['sbp_medication'].value_counts().to_string())
print()
print('visit_type value counts:')
print(pop_after['visit_type'].value_counts().to_string())
print()
print('disease state joint counts (top 8):')
joint = (
    pop_after.groupby(['ischemic_stroke',
                        'ischemic_heart_disease_and_heart_failure'])
    .size()
    .sort_values(ascending=False)
    .head(8)
)
print(joint.to_string())
"""
    )
)

cells.append(
    md(
        """\
## 5. Verdict

If this notebook ran end-to-end without exceptions and:

- Treatment + HealthcareUtilization columns initialize with
  realistic baseline distributions (most on `no_treatment`, a
  minority on the lower medication rungs, some emergency-state
  simulants flagged at t=0).
- Adults on SBP / LDL-C medications carry a multiplier > 1 so
  that `risk.exposure` (the untreated value used in risk
  calculations) sits above `risk.gbd_exposure`.
- Multiple 28-day steps run cleanly and the visit / medication /
  disease-state distributions evolve (rather than staying frozen).

then **Phase 4 is complete**. The remaining work in Phase 5 is
wiring the full observer suite back in on top of this stack.
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

out = Path(__file__).parent / "08_phase4_healthcare_treatment.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
