"""Generate notebooks/05_phase1_disease_models.ipynb.

Running this script writes a fresh, unexecuted copy of the Phase 1
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
# Phase 1: Disease Models Only

The vivarium 4 / vivarium_public_health 5 upgrade is being built up
incrementally. This notebook is the **Phase 1** acceptance test:

> **Goal:** load the smallest possible model spec — disease models +
> base population + a mortality observer — call `setup()`, step the
> simulation, and confirm that the disease state machines initialize
> from prevalence and that simulants transition between states.

If this notebook fails, none of the later phases (risk factors, risk
effects, healthcare/treatment, observers) can possibly work. So this
is the gate to clear before adding more components.

The model spec used here is
`src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase1.yaml`.
It loads:

- `BasePopulation` (vivarium_public_health)
- `MortalityObserver` (vivarium_public_health)
- The two disease models from `causes.yaml` parsed by
  `CausesConfigurationParser`:
    - `ischemic_stroke` (susceptible, acute, chronic)
    - `ischemic_heart_disease_and_heart_failure` (susceptible, acute MI,
      post MI, HF from IHD, acute MI + HF, HF residual)

Two patches in `plugins/__init__.py` are exercised here for the first
time:

1. **`ArtifactManager.load` draw-column stripping** — vivarium 4
   renames `draw_0` → `value` but leaves `draw_1`…`draw_249`, and the
   leftover columns get treated as categorical key columns by
   `LookupTable`. The patch drops them after load.
2. **`risk_distributions.EnsembleDistribution.get_expected_parameters`**
   shim — vph 5 expects this classmethod, but it doesn't exist on
   `risk_distributions 2.1.3`. (Not directly exercised in Phase 1
   since there are no risks, but the import happens at parser load
   time.)
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

MODEL_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase1.yaml'
"""
    )
)

cells.append(
    md(
        """\
## 1. Set up the simulation

If `InteractiveContext(...)` returns without raising, the artifact
loaded cleanly, the disease models built their lookup tables, and the
initial population was sampled from prevalence.

Note that the new `vivarium 4` `get_population` API requires an explicit
list of attribute names. The `is_alive` boolean column replaces the old
`alive` string column.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(MODEL_SPEC)
print('setup OK')
print('current time:', sim.current_time)
print('configured pop size:', sim.configuration.population.population_size)
"""
    )
)

cells.append(
    md(
        """\
## 2. Initial population overview

The model spec initializes 1,000 simulants ages 5-125. Disease state
columns are sampled from artifact prevalence at simulation start.
"""
    )
)

cells.append(
    code(
        """\
ATTRS = [
    'age', 'sex', 'is_alive', 'cause_of_death',
    'ischemic_stroke', 'ischemic_heart_disease_and_heart_failure',
]

pop0 = sim.get_population(ATTRS)
print('shape:', pop0.shape)
print()
print('age summary:')
print(pop0['age'].describe().round(2))
print()
print('sex:')
print(pop0['sex'].value_counts())
print()
print('alive:')
print(pop0['is_alive'].value_counts())
"""
    )
)

cells.append(
    md(
        """\
## 3. Initial disease state distribution

The two cause models should each have most simulants in the
`susceptible_to_*` state, with a small fraction sampled into each
diseased state from the artifact prevalence.
"""
    )
)

cells.append(
    code(
        """\
print('--- ischemic_stroke ---')
print(pop0['ischemic_stroke'].value_counts())
print()
print('--- ischemic_heart_disease_and_heart_failure ---')
print(pop0['ischemic_heart_disease_and_heart_failure'].value_counts())
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 2, figsize=(14, 4))

is_counts = pop0['ischemic_stroke'].value_counts()
axes[0].barh(is_counts.index.astype(str), is_counts.values, color='steelblue')
axes[0].set_title('Ischemic stroke (initial)')
axes[0].set_xlabel('count')

ihd_counts = pop0['ischemic_heart_disease_and_heart_failure'].value_counts()
axes[1].barh(ihd_counts.index.astype(str), ihd_counts.values, color='indianred')
axes[1].set_title('IHD + HF (initial)')
axes[1].set_xlabel('count')

plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 4. Step the simulation forward

We run for one full simulated year (~13 steps of 28 days each). The
key thing this exercises is that:

- The `RateTransition` machinery built by the cause parser actually
  fires and moves simulants through state transitions.
- The mortality model retires dead simulants without crashing.
- No lookup table interpolation errors occur from leftover `draw_*`
  columns (the monkey-patch is doing its job).
"""
    )
)

cells.append(
    code(
        """\
N_STEPS = 13
for _ in range(N_STEPS):
    sim.step()
print('current time:', sim.current_time)

pop1 = sim.get_population(ATTRS)
print('alive after one year:', int(pop1['is_alive'].sum()), '/', len(pop1))
"""
    )
)

cells.append(
    code(
        """\
print('--- ischemic_stroke ---')
print(pop1['ischemic_stroke'].value_counts())
print()
print('--- ischemic_heart_disease_and_heart_failure ---')
print(pop1['ischemic_heart_disease_and_heart_failure'].value_counts())
"""
    )
)

cells.append(
    md(
        """\
## 5. State transitions across the year

Compare counts at t=0 and t=1y to verify simulants actually moved
between states (incidence + dwell-time + remission transitions all
fire).
"""
    )
)

cells.append(
    code(
        """\
def state_change_table(name: str) -> pd.DataFrame:
    a = pop0[name].value_counts()
    b = pop1[name].value_counts()
    df = pd.DataFrame({'t=0': a, 't=1y': b}).fillna(0).astype(int)
    df['delta'] = df['t=1y'] - df['t=0']
    return df.sort_values('t=0', ascending=False)

print('--- ischemic_stroke ---')
print(state_change_table('ischemic_stroke'))
print()
print('--- ischemic_heart_disease_and_heart_failure ---')
print(state_change_table('ischemic_heart_disease_and_heart_failure'))
"""
    )
)

cells.append(
    md(
        """\
## 6. Mortality across the year

The mortality model is firing if some simulants died from cardiac
causes (or `other_causes`) over the year.
"""
    )
)

cells.append(
    code(
        """\
print('cause_of_death after one year:')
print(pop1['cause_of_death'].value_counts())
print()
deaths = pop1[~pop1['is_alive']]
print(f'{len(deaths)} deaths total ({100 * len(deaths) / len(pop1):.1f}%)')
print()
print('mean age at death:')
print(deaths.groupby('cause_of_death')['age'].mean().round(1))
"""
    )
)

cells.append(
    md(
        """\
## 7. Verdict

If you reached this cell with no exceptions and:

- The disease state value counts above show non-trivial counts in the
  diseased states (initial prevalence sampling worked),
- The `delta` column in the state-change table shows that some
  simulants actually moved between states across the year (transitions
  fired),
- A reasonable number of deaths accumulated,

then **Phase 1 is complete** and the upgrade is ready to start adding
risk factors (Phase 2).
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

out = Path(__file__).parent / "05_phase1_disease_models.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out}")
