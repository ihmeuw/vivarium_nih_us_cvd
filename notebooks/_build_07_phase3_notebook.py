"""Generate notebooks/07_phase3_risk_effects.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 3
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
# Phase 3: Risk Effects (Log-Linear and Non-Log-Linear)

Phase 3 attaches two flavors of risk effect to the disease
transition rates:

**Log-linear** (`RiskEffectWithoutPAF`): the GBD 2023 USA artifact
stores BMI relative risks as log-linear "per unit" values
affecting two heart-failure causes:

- `heart_failure_from_ischemic_heart_disease.incidence_rate`
- `heart_failure_residual.incidence_rate`

**Non-log-linear** (`NonLogLinearRiskEffectWithoutPAF`): the
artifact stores SBP relative risks as a 200,000-row long-form
table with 1000 distinct exposure-level `parameter` values and a
monotonically rising RR. vph 5's `NonLogLinearRiskEffect` reads
that table directly, builds piecewise-linear interpolation
intervals, anchors them on the TMRED-sampled TMREL, and looks up
each simulant's RR from their cached SBP exposure. This notebook
demonstrates the `SBP -> acute_ischemic_stroke.incidence_rate`
effect.

The model spec used here is
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase3.yaml`.
It is Phase 2 plus three risk-effect declarations from the
project's own `effects.py`.

What this notebook checks:

1. The `RiskEffectWithoutPAF` subclass loads its log-linear RR
   table cleanly even though the artifact tags the rows with a
   single-value `parameter='per unit'` index level.
2. The `NonLogLinearRiskEffectWithoutPAF` subclass loads the
   exposure-indexed RR table for SBP and registers the cached
   exposure column needed for interval lookup.
3. The relative-risk pipelines `<risk>_on_<target>.relative_risk`
   are registered and produce reasonable per-simulant values.
4. **Log-linear**: BMI -> HF RR is monotone in exposure with the
   characteristic flat tail at RR = 1 below TMREL.
5. **Non-log-linear**: SBP -> AIS RR is monotone in exposure and
   tracks the artifact's tabulated dose-response curve (rising
   from 1.0 at TMREL up to >10x at high SBP).
6. The downstream `<target>.incidence_rate` pipeline reflects the
   RR adjustment (compared against the unmodified base rate from
   Phase 2 / artifact data).
7. Stepping the simulation does not crash.
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

PHASE2_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase2.yaml'
PHASE3_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase3.yaml'

HF_TARGETS = [
    'heart_failure_from_ischemic_heart_disease',
    'heart_failure_residual',
]
RR_PIPELINES = [
    f'high_body_mass_index_in_adults_on_{t}.relative_risk' for t in HF_TARGETS
]
RATE_PIPELINES = [f'{t}.incidence_rate' for t in HF_TARGETS]

# Non-log-linear effect: SBP -> acute ischemic stroke
SBP_EXPOSURE = 'high_systolic_blood_pressure.exposure'
SBP_RR_PIPELINE = 'high_systolic_blood_pressure_on_acute_ischemic_stroke.relative_risk'
AIS_RATE_PIPELINE = 'acute_ischemic_stroke.incidence_rate'
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up the simulation

If `InteractiveContext(...)` returns without raising, the new
`RiskEffectWithoutPAF` components have:

- loaded the `risk_factor.high_body_mass_index_in_adults.relative_risk`
  table from the artifact (250-draw long form),
- stripped the single-value `parameter='per unit'` index level
  before binning,
- registered the relative-risk value pipelines, and
- registered a target rate modifier on each heart-failure
  incidence rate.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(PHASE3_SPEC)
print('setup OK')
print('current time:', sim.current_time)
"""
    )
)

cells.append(
    md(
        """\
## 2. Find the new pipelines

The relative-risk pipelines should appear under
`<risk>_on_<target>.relative_risk`.
"""
    )
)

cells.append(
    code(
        """\
attrs = sim.get_attribute_names()
new_pipes = [a for a in sorted(attrs) if 'on_heart_failure' in a]
for a in new_pipes:
    print(' ', a)
"""
    )
)

cells.append(
    md(
        """\
## 3. Pull the population view

We grab BMI exposure, both relative-risk pipelines, and both
heart-failure incidence-rate pipelines for the simulants.
"""
    )
)

cells.append(
    code(
        """\
ATTRS = ['age', 'sex', 'is_alive']
ATTRS += ['high_body_mass_index_in_adults.exposure']
ATTRS += [SBP_EXPOSURE]
ATTRS += RR_PIPELINES
ATTRS += [SBP_RR_PIPELINE]
ATTRS += RATE_PIPELINES
ATTRS += [AIS_RATE_PIPELINE]

pop = sim.get_population(ATTRS)
adults = pop[pop['age'] >= 25].copy()
print(f'{len(adults)} adults out of {len(pop)} simulants')
print()
print(adults.head())
"""
    )
)

cells.append(
    md(
        """\
## 4. Relative risk distribution

A log-linear `per unit` BMI effect predicts
`RR_i = exp(beta * (bmi_i - tmrel))` for `bmi_i > tmrel` and
`RR_i = 1` otherwise. So we expect a degenerate-at-1 lower bound
(simulants below TMREL) and a long right tail.
"""
    )
)

cells.append(
    code(
        """\
print('BMI->HF_IHD relative risk:')
print(adults[RR_PIPELINES[0]].describe().round(3))
print()
print('BMI->HF_residual relative risk:')
print(adults[RR_PIPELINES[1]].describe().round(3))
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for ax, target, pipe in zip(axes, HF_TARGETS, RR_PIPELINES):
    ax.hist(adults[pipe], bins=40, color='steelblue', edgecolor='white')
    ax.axvline(1.0, color='k', linestyle='--', alpha=0.6, label='TMREL (RR=1)')
    ax.set_title(f'BMI -> {target}')
    ax.set_xlabel('relative risk')
    ax.set_ylabel('count')
    ax.legend()
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 5. Relative risk vs. BMI exposure

For a log-linear effect we should see a clean monotone relationship
between BMI exposure and the per-simulant relative risk: flat at
RR = 1 below the TMREL anchor and rising above it.
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
for ax, target, pipe in zip(axes, HF_TARGETS, RR_PIPELINES):
    ax.scatter(
        adults['high_body_mass_index_in_adults.exposure'],
        adults[pipe],
        s=8, alpha=0.4, color='steelblue',
    )
    ax.axhline(1.0, color='k', linestyle='--', alpha=0.6)
    ax.set_title(f'BMI -> {target}')
    ax.set_xlabel('BMI (kg/m^2)')
    ax.set_ylabel('relative risk')
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 6. Cross-check vs. Phase 2 (no risk effect)

Phase 2 has the same disease and risk components but no
`RiskEffectWithoutPAF`. The heart-failure incidence rates pulled
from Phase 2 should equal the artifact baseline. The Phase 3 rates
should equal `baseline * RR` (with the same simulants since both
specs use seed 0 and identical population settings).
"""
    )
)

cells.append(
    code(
        """\
sim2 = InteractiveContext(PHASE2_SPEC)
ATTRS2 = (
    ['age', 'sex', 'is_alive',
     'high_body_mass_index_in_adults.exposure',
     SBP_EXPOSURE]
    + RATE_PIPELINES
    + [AIS_RATE_PIPELINE]
)
pop2 = sim2.get_population(ATTRS2)
adults2 = pop2[pop2['age'] >= 25].copy()
print(f'phase2 adults: {len(adults2)}')
print()
print('phase2 baseline HF_IHD incidence rate:')
print(adults2[RATE_PIPELINES[0]].describe())
print()
print('phase2 baseline AIS incidence rate:')
print(adults2[AIS_RATE_PIPELINE].describe())
"""
    )
)

cells.append(
    code(
        """\
common = adults.index.intersection(adults2.index)
print(f'{len(common)} simulants present in both phases')
print()
ratios = pd.DataFrame(index=common)
for target, pipe in zip(HF_TARGETS, RATE_PIPELINES):
    base = adults2.loc[common, pipe]
    boosted = adults.loc[common, pipe]
    nonzero = base > 0
    ratios[target] = np.nan
    ratios.loc[nonzero, target] = boosted[nonzero] / base[nonzero]

# Compare ratio (phase3 rate / phase2 rate) to the relative-risk pipeline.
for target, rr_pipe in zip(HF_TARGETS, RR_PIPELINES):
    rr = adults.loc[common, rr_pipe]
    ratio = ratios[target]
    diff = (ratio - rr).abs()
    print(f'{target}: max |rate_ratio - RR| = {diff.max():.6f}, '
          f'mean = {diff.mean():.6f}')
"""
    )
)

cells.append(
    md(
        """\
If the max absolute difference is essentially zero, the risk
effect is being applied multiplicatively as expected.
"""
    )
)

cells.append(
    md(
        """\
## 7. Non-log-linear effect: SBP -> acute ischemic stroke

The next section exercises the **non-log-linear** branch:
`NonLogLinearRiskEffectWithoutPAF` reads the artifact's
exposure-indexed RR table for SBP and looks up each simulant's RR
based on their cached SBP exposure (using piecewise-linear
interpolation between the tabulated parameter values, anchored on
the TMRED-sampled TMREL).

Unlike the log-linear case, the dose-response curve is not a
simple `exp(beta * delta)` — it follows whatever shape the GBD
relative risk surface has. We expect:

- RR = 1 at SBP at or below the TMREL anchor (~110-115 mmHg).
- A monotone, accelerating rise as SBP increases.
- Mean RR by SBP bin should match the artifact's tabulated curve,
  reaching values above 10x for SBP in the 200+ range.
"""
    )
)

cells.append(
    code(
        """\
print('SBP -> AIS relative risk:')
print(adults[SBP_RR_PIPELINE].describe().round(3))
print()
print('SBP exposure:')
print(adults[SBP_EXPOSURE].describe().round(2))
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 2, figsize=(12, 4))

# Histogram of RR values
axes[0].hist(adults[SBP_RR_PIPELINE], bins=40,
             color='darkorange', edgecolor='white')
axes[0].axvline(1.0, color='k', linestyle='--', alpha=0.6,
                label='TMREL (RR=1)')
axes[0].set_title('SBP -> AIS relative risk distribution')
axes[0].set_xlabel('relative risk')
axes[0].set_ylabel('count')
axes[0].legend()

# Scatter: RR vs SBP exposure
axes[1].scatter(
    adults[SBP_EXPOSURE],
    adults[SBP_RR_PIPELINE],
    s=8, alpha=0.4, color='darkorange',
)
axes[1].axhline(1.0, color='k', linestyle='--', alpha=0.6)
axes[1].set_title('SBP -> AIS dose-response (per simulant)')
axes[1].set_xlabel('SBP (mmHg)')
axes[1].set_ylabel('relative risk')
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
### 7a. Mean RR by SBP bin

Bin simulants by SBP exposure in 10 mmHg increments and look at
the mean RR per bin. This is the cleanest view of the underlying
dose-response curve — it should rise smoothly from 1.0 at the
TMREL anchor up to >10x at high SBP.
"""
    )
)

cells.append(
    code(
        """\
sbp = adults[SBP_EXPOSURE]
rr = adults[SBP_RR_PIPELINE]

bins = np.arange(70, 220, 10)
bin_labels = pd.cut(sbp, bins=bins)
dose_response = (
    pd.DataFrame({'sbp_bin': bin_labels, 'rr': rr})
    .groupby('sbp_bin', observed=True)['rr']
    .agg(['mean', 'min', 'max', 'count'])
    .round(3)
)
print(dose_response)
"""
    )
)

cells.append(
    code(
        """\
fig, ax = plt.subplots(figsize=(10, 5))

# Per-simulant scatter (background)
ax.scatter(sbp, rr, s=6, alpha=0.2, color='lightgray',
           label='per simulant')

# Mean RR by SBP bin (foreground)
bin_centers = [interval.mid for interval in dose_response.index]
ax.plot(bin_centers, dose_response['mean'], 'o-',
        color='darkorange', linewidth=2, markersize=8,
        label='mean RR per 10 mmHg bin')
ax.fill_between(
    bin_centers,
    dose_response['min'],
    dose_response['max'],
    alpha=0.2, color='darkorange',
    label='min/max in bin',
)

ax.axhline(1.0, color='k', linestyle='--', alpha=0.6,
           label='RR = 1')
ax.set_title('Non-log-linear dose-response: SBP -> AIS incidence')
ax.set_xlabel('SBP (mmHg)')
ax.set_ylabel('relative risk')
ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
### 7b. Cross-check vs Phase 2 for AIS

Same multiplicative-application check as we did for the log-linear
BMI effect: the Phase 3 AIS incidence rate should equal the Phase 2
baseline AIS rate times the SBP-on-AIS RR pipeline value, simulant
by simulant.
"""
    )
)

cells.append(
    code(
        """\
common = adults.index.intersection(adults2.index)
base_ais = adults2.loc[common, AIS_RATE_PIPELINE]
boosted_ais = adults.loc[common, AIS_RATE_PIPELINE]
sbp_rr = adults.loc[common, SBP_RR_PIPELINE]

nonzero = base_ais > 0
ratio = boosted_ais[nonzero] / base_ais[nonzero]
diff = (ratio - sbp_rr[nonzero]).abs()
print(f'AIS: max |rate_ratio - RR| = {diff.max():.6f}, '
      f'mean = {diff.mean():.6f}')
print()
print('phase2 AIS rate (baseline):')
print(base_ais.describe().round(6))
print()
print('phase3 AIS rate (with SBP effect):')
print(boosted_ais.describe().round(6))
"""
    )
)

cells.append(
    md(
        """\
## 8. Step the simulation forward

Run for one simulated year and confirm the disease state machines
still fire (a regression check on top of Phases 1 and 2).
"""
    )
)

cells.append(
    code(
        """\
for _ in range(13):  # ~1 year
    sim.step()
print('current time:', sim.current_time)

pop_after = sim.get_population(ATTRS)
print('alive after 1y:', int(pop_after['is_alive'].sum()), '/', len(pop_after))
"""
    )
)

cells.append(
    code(
        """\
adults_after = pop_after[pop_after['age'] >= 25].copy()
print('BMI->HF_IHD RR (after 1y):')
print(adults_after[RR_PIPELINES[0]].describe().round(3))
"""
    )
)

cells.append(
    md(
        """\
## 9. Verdict

If you reached this cell with no exceptions and:

**Log-linear (BMI -> heart failure):**

- Both `high_body_mass_index_in_adults_on_<hf_target>.relative_risk`
  pipelines registered,
- Adult RR distributions are bounded below by 1.0 with a long
  right tail,
- RR rises monotonically with BMI exposure (flat at 1.0 below
  TMREL),
- The Phase 3 HF incidence rate equals Phase 2 baseline times the
  RR pipeline value to within numerical noise.

**Non-log-linear (SBP -> acute ischemic stroke):**

- The
  `high_systolic_blood_pressure_on_acute_ischemic_stroke.relative_risk`
  pipeline registered,
- Mean RR by 10 mmHg SBP bin shows a clean monotone dose-response
  curve, flat near 1.0 around the TMREL anchor (~110-115 mmHg) and
  rising past 10x at high SBP,
- The Phase 3 AIS incidence rate equals Phase 2 baseline times the
  SBP-on-AIS RR pipeline value to within numerical noise.

**General:**

- One simulated year of stepping completes without errors.

then **Phase 3 is complete** for both log-linear and non-log-linear
risk effects without per-risk PAFs. The remaining work in later
phases is joint PAF computation, the mediation pipeline (BMI -> HF
via categorical SBP), and adding the LDL-C and FPG non-log-linear
effects (which use the same machinery exercised here).
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

out = Path(__file__).parent / "07_phase3_risk_effects.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
