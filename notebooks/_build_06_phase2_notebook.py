"""Generate notebooks/06_phase2_risk_factors.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 2
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
# Phase 2: Add Risk Factors

Phase 2 of the upgrade adds the four continuous risk factors that
drive cardiovascular outcomes in the model:

| Risk | Units | Distribution |
|------|-------|--------------|
| High systolic blood pressure | mmHg | Ensemble |
| High LDL cholesterol | mmol/L | Ensemble |
| High BMI (adults) | kg/m^2 | Ensemble |
| High fasting plasma glucose | mmol/L | Ensemble |

The model spec used here is
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase2.yaml`.
It is exactly Phase 1 plus four `Risk(...)` declarations from
`vivarium_public_health` for the risks above. The project's custom
`AdjustedRisk` / `TruncatedRisk` / `CorrelatedRisk` subclasses are
NOT used in this phase: they depend on multiplier columns created
by the `Treatment` component which is introduced in Phase 4.

What this notebook checks:

1. The four `Risk` components register cleanly with the new
   GBD 2023 artifact (which has 250 draws and the `parameter`
   index level on the exposure tables).
2. The propensity columns are created at initialization and the
   exposure pipelines return reasonable distributions for adults.
3. Stepping the simulation does not crash and does not change
   propensities (they should be fixed at birth/initialization).
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
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 200)

MODEL_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase2.yaml'

RISK_NAMES = [
    'high_systolic_blood_pressure',
    'high_ldl_cholesterol',
    'high_body_mass_index_in_adults',
    'high_fasting_plasma_glucose',
]
RISK_UNITS = {
    'high_systolic_blood_pressure': 'mmHg',
    'high_ldl_cholesterol': 'mmol/L',
    'high_body_mass_index_in_adults': 'kg/m^2',
    'high_fasting_plasma_glucose': 'mmol/L',
}
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up the simulation

If `InteractiveContext(...)` returns without raising, the four risk
factor components have:

- loaded their exposure / standard deviation / distribution weight
  tables from the artifact,
- built `EnsembleDistribution` objects, and
- registered the propensity initializers and exposure pipelines.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(MODEL_SPEC)
print('setup OK')
print('current time:', sim.current_time)
"""
    )
)

cells.append(
    md(
        """\
## 2. Inspect registered risk pipelines

vph 5 registers a `<risk>.exposure` value pipeline (and a
`<risk>.exposure.propensity` propensity column) for every continuous
risk. We list them here as proof that the components wired
themselves up.
"""
    )
)

cells.append(
    code(
        """\
risk_attrs = [
    a for a in sim.get_attribute_names()
    if any(r in a for r in RISK_NAMES)
]
for a in sorted(risk_attrs):
    print(' ', a)
"""
    )
)

cells.append(
    md(
        """\
## 3. Pull a population view including all risk exposures and propensities
"""
    )
)

cells.append(
    code(
        """\
ATTRS = ['age', 'sex', 'is_alive']
ATTRS += [f'{r}.exposure' for r in RISK_NAMES]
ATTRS += [f'{r}.propensity' for r in RISK_NAMES]
ATTRS += ['ischemic_stroke', 'ischemic_heart_disease_and_heart_failure']

pop = sim.get_population(ATTRS)
print('shape:', pop.shape)
print()
print(pop.head())
"""
    )
)

cells.append(
    md(
        """\
## 4. Risk exposure summary statistics

Adults (age >= 25) are the cohort for which the artifact has
non-trivial exposure data. Children carry zeros / placeholder values.
Exclude them when summarising.
"""
    )
)

cells.append(
    code(
        """\
adults = pop[pop['age'] >= 25].copy()
print(f'{len(adults)} adults out of {len(pop)} simulants')
print()

summary = adults[[f'{r}.exposure' for r in RISK_NAMES]].describe().round(2).T
summary.index = [s.replace('.exposure', '') for s in summary.index]
summary['units'] = [RISK_UNITS[r] for r in summary.index]
print(summary[['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max', 'units']])
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(2, 2, figsize=(12, 7))
for ax, risk in zip(axes.ravel(), RISK_NAMES):
    vals = adults[f'{risk}.exposure']
    ax.hist(vals, bins=40, color='steelblue', edgecolor='white')
    ax.set_title(f'{risk} ({RISK_UNITS[risk]})')
    ax.set_xlabel('exposure')
    ax.set_ylabel('count')
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 5. Propensity sanity check

Without `RiskCorrelation` (added in a later phase), each risk's
propensity is sampled independently from a uniform(0, 1).
"""
    )
)

cells.append(
    code(
        """\
prop_cols = [f'{r}.propensity' for r in RISK_NAMES]
prop_summary = pop[prop_cols].describe().round(3).T
prop_summary.index = [s.replace('.propensity', '') for s in prop_summary.index]
print(prop_summary[['count', 'mean', 'std', 'min', '50%', 'max']])
print()

# Pairwise correlation should be ~0 (independent uniforms).
corr = pop[prop_cols].corr().round(3)
corr.index = corr.columns = [c.replace('.propensity', '') for c in corr.columns]
print('propensity correlation matrix:')
print(corr)
"""
    )
)

cells.append(
    md(
        """\
## 6. Sex- and age-stratified mean exposure

These should look roughly like the artifact's marginal exposure
means: mean SBP rising into older ages, BMI peaking around middle
age, etc.
"""
    )
)

cells.append(
    code(
        """\
adults['age_bin'] = pd.cut(
    adults['age'],
    bins=[25, 35, 45, 55, 65, 75, 85, 95, 125],
    right=False,
)

fig, axes = plt.subplots(2, 2, figsize=(12, 7))
for ax, risk in zip(axes.ravel(), RISK_NAMES):
    means = (
        adults
        .groupby(['age_bin', 'sex'], observed=True)[f'{risk}.exposure']
        .mean()
        .unstack('sex')
    )
    means.plot(ax=ax, marker='o')
    ax.set_title(risk)
    ax.set_ylabel(f'mean ({RISK_UNITS[risk]})')
    ax.set_xlabel('age bin')
    ax.tick_params(axis='x', rotation=30)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 7. Step the simulation forward

Run for one simulated year and confirm:

- the disease state machines still fire (Phase 1 invariant),
- propensities stay constant (they're set once at initialization),
- exposures may shift slightly due to age changes / table
  interpolation across step times.
"""
    )
)

cells.append(
    code(
        """\
prop_before = pop[prop_cols].copy()
exposures_before = pop[[f'{r}.exposure' for r in RISK_NAMES]].copy()

for _ in range(13):  # ~1 year
    sim.step()
print('current time:', sim.current_time)

pop2 = sim.get_population(ATTRS)
print('alive after 1y:', int(pop2['is_alive'].sum()), '/', len(pop2))
"""
    )
)

cells.append(
    code(
        """\
common = pop.index.intersection(pop2.index)
prop_changed = (pop2.loc[common, prop_cols] != prop_before.loc[common]).any().any()
print('any propensity changed?', prop_changed)
print()

# Exposure means before / after
mean_before = pop.loc[pop['age'] >= 25, [f'{r}.exposure' for r in RISK_NAMES]].mean().round(2)
mean_after  = pop2.loc[pop2['age'] >= 25, [f'{r}.exposure' for r in RISK_NAMES]].mean().round(2)
delta = (mean_after - mean_before).round(3)
print(pd.DataFrame({'before': mean_before, 'after_1y': mean_after, 'delta': delta}))
"""
    )
)

cells.append(
    md(
        """\
## 8. Verdict

If you reached this cell with no exceptions and:

- The four risk factor components registered without errors,
- Adult exposure summaries look physiologically reasonable
  (e.g. mean SBP in the 120s mmHg, mean LDL-C in the 2s-3s mmol/L,
  mean BMI in the 25-30 kg/m^2 range, mean FPG in the 5s-6s mmol/L),
- The propensity values are uniform-ish on (0, 1) and uncorrelated
  pairwise (no `RiskCorrelation` yet),
- One year of stepping completes without errors and propensities
  stay frozen,

then **Phase 2 is complete**. The next phase will hook up
`MediatedRiskEffect` so that the relative-risk artifact data is
applied to disease transition rates.
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

out = Path(__file__).parent / "06_phase2_risk_factors.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
