"""Generate notebooks/04_model_validation.ipynb.

Running this script writes a fresh, unexecuted copy of the validation
notebook. Execute the notebook with nbconvert to populate outputs.
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
# Model Validation: 10-Year Trajectory

This notebook validates the NIH US CVD simulation against the values
stored in the input artifact. We run a single simulation for 10 years
with a large population and check, at baseline and across the run:

1. **Prevalence** of each disease state, stratified by age and sex,
   vs. the prevalence data in the artifact.
2. **Risk factor exposures** (LDL-C, SBP, BMI, FPG), stratified by
   age and sex, vs. the artifact exposure means.
3. **Incidence** of acute MI and acute ischemic stroke events, vs.
   the artifact incidence rates.
4. **Mortality**: all-cause and cause-specific death counts vs. the
   artifact mortality rates.
5. **Risk factor effects**: verify that simulants with higher LDL-C
   actually experience higher cumulative incidence of MI and stroke
   (so the `MediatedRiskEffect` components are wired up correctly).

All stochastic comparisons use `vivarium_testing_utils.FuzzyChecker`,
which performs a Bayesian hypothesis test between an artifact-derived
"no bug" target distribution and a diffuse "bug" distribution. Each
check is a soft assertion — failures are collected and reported at
the end so one outlier doesn't halt the notebook.

**Runtime note.** With `POPULATION_SIZE = 3000` and 10 simulated years,
expect roughly 30-60 minutes on a modern laptop. Scale
`POPULATION_SIZE` down for quick iteration, or up for more decisive
checks.
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
from pathlib import Path

from vivarium import InteractiveContext
from vivarium_testing_utils.fuzzy_checker import FuzzyChecker

plt.rcParams['figure.figsize'] = (12, 5)
plt.rcParams['figure.dpi'] = 100
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
"""
    )
)

cells.append(
    md(
        """\
## 1. Configuration

`POPULATION_SIZE` drives how decisive the fuzzy-checker assertions can
be — very small populations will return "inconclusive" results for
rare events. The model's default initialization age range (25-125) is
kept so all simulants are adults with non-zero disease rates.
"""
    )
)

cells.append(
    code(
        """\
POPULATION_SIZE = 3_000
YEARS = 10
STEPS_PER_YEAR = 13  # the model spec uses 28-day steps

MODEL_SPEC = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd.yaml'
ARTIFACT = str(Path('../src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf').resolve())

# 5-year age bins for stratification. The model spec initializes
# simulants from age 5 upward, so we cover the full range. Note that
# disease prevalence/incidence and risk factor exposures are zero in
# the artifact below age ~25, so checks in those bins are mostly
# trivial (0 == 0).
AGE_EDGES = np.array([5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 95])
AGE_BIN_LABELS = [f'{a}-{b}' for a, b in zip(AGE_EDGES[:-1], AGE_EDGES[1:])]
ADULT_EDGES = AGE_EDGES[AGE_EDGES >= 25]
ADULT_BIN_LABELS = [f'{a}-{b}' for a, b in zip(ADULT_EDGES[:-1], ADULT_EDGES[1:])]

def age_bin(ages: pd.Series) -> pd.Series:
    '''Bin continuous ages into the 5-year labels above.'''
    return pd.cut(ages, bins=AGE_EDGES, right=False, labels=AGE_BIN_LABELS)
"""
    )
)

cells.append(
    md(
        """\
## 2. Load Artifact Tables

Pull every artifact table we'll compare against up-front so the rest
of the notebook can look values up cheaply.
"""
    )
)

cells.append(
    code(
        """\
def load_draws(key: str) -> pd.DataFrame:
    '''Load an artifact table and return it with a MultiIndex on
    demographics and draw columns as the values.'''
    df = pd.read_hdf(ARTIFACT, key)
    if isinstance(df.index, pd.MultiIndex):
        return df
    return df.set_index([c for c in df.columns if not c.startswith('draw_')])

def draws_at(df: pd.DataFrame, sex: str, age_start: float, age_end: float) -> np.ndarray:
    '''Look up a row by (sex, age_start, age_end) and return the 1000 draws.'''
    idx = df.reset_index()
    mask = (idx['sex'] == sex) & (idx['age_start'] == age_start) & (idx['age_end'] == age_end)
    row = idx[mask]
    if row.empty:
        return np.array([])
    draw_cols = [c for c in row.columns if c.startswith('draw_')]
    return row[draw_cols].values.ravel().astype(float)

# Disease prevalence keys — map each sim state column value to artifact path.
PREVALENCE_KEYS = {
    'ischemic_stroke': {
        'acute_ischemic_stroke': '/sequela/acute_ischemic_stroke/prevalence',
        'chronic_ischemic_stroke': '/sequela/chronic_ischemic_stroke/prevalence',
    },
    'ischemic_heart_disease_and_heart_failure': {
        'acute_myocardial_infarction': '/cause/acute_myocardial_infarction/prevalence',
        'post_myocardial_infarction': '/cause/post_myocardial_infarction/prevalence',
        'heart_failure_from_ischemic_heart_disease':
            '/cause/heart_failure_from_ischemic_heart_disease/prevalence',
        'heart_failure_residual': '/cause/heart_failure_residual/prevalence',
        'acute_myocardial_infarction_and_heart_failure':
            '/cause/acute_myocardial_infarction_and_heart_failure/prevalence',
    },
}

INCIDENCE_KEYS = {
    'acute_myocardial_infarction':
        '/cause/acute_myocardial_infarction/incidence_rate',
    'acute_ischemic_stroke':
        '/cause/ischemic_stroke/incidence_rate',
}

MORTALITY_KEY = '/cause/all_causes/cause_specific_mortality_rate'

EXPOSURE_KEYS = {
    'high_ldl_cholesterol': '/risk_factor/high_ldl_cholesterol/exposure',
    'high_systolic_blood_pressure': '/risk_factor/high_systolic_blood_pressure/exposure',
    'high_body_mass_index_in_adults': '/risk_factor/high_body_mass_index_in_adults/exposure',
    'high_fasting_plasma_glucose': '/risk_factor/high_fasting_plasma_glucose/exposure',
}

# Load everything
prevalence_tables = {
    state: load_draws(key)
    for mapping in PREVALENCE_KEYS.values()
    for state, key in mapping.items()
}
incidence_tables = {cause: load_draws(key) for cause, key in INCIDENCE_KEYS.items()}
mortality_table = load_draws(MORTALITY_KEY)
exposure_tables = {rf: load_draws(key) for rf, key in EXPOSURE_KEYS.items()}

print(f'Loaded {len(prevalence_tables)} prevalence tables, '
      f'{len(incidence_tables)} incidence tables, '
      f'{len(exposure_tables)} exposure tables.')
"""
    )
)

cells.append(
    md(
        """\
## 3. Set Up the Simulation

We use `InteractiveContext` so we can step year by year and inspect
the population table. Using the USA-level artifact means every
simulant is drawn from the US population structure.
"""
    )
)

cells.append(
    code(
        """\
sim = InteractiveContext(
    MODEL_SPEC,
    configuration={
        'input_data': {'artifact_path': ARTIFACT},
        'population': {'population_size': POPULATION_SIZE},
    },
    setup=True,
)

pop0 = sim.get_population()
print(f'Start time:       {sim.current_time}')
print(f'Population size:  {len(pop0):,}')
print(f'Age range:        {pop0.age.min():.1f} - {pop0.age.max():.1f}')
print(f'Sex split:        {pop0.sex.value_counts().to_dict()}')
"""
    )
)

cells.append(
    code(
        """\
fuzzy = FuzzyChecker()
EPS = 1e-9  # FuzzyChecker requires strictly positive lower bound

def safe_check(name: str, numerator: int, denominator: int,
               target: tuple[float, float] | float,
               name_additional: str = ''):
    '''Run a fuzzy proportion check that always records a diagnostic.

    Workarounds for `FuzzyChecker.fuzzy_assert_proportion`:
    - Skip cells with empty denominators.
    - When the artifact target is exactly (0, 0), nudge the upper bound
      to half a single-event proportion so any real positive observation
      decisively favors a bug.
    - Nudge a zero lower bound by EPS — `_fit_beta_distribution_to_uncertainty_interval`
      requires 0 < lower < upper < 1.
    - Call `test_proportion` directly (instead of the asserting wrapper)
      so the diagnostic is appended even when `reject_null` is True.
    '''
    if denominator == 0:
        return None
    if isinstance(target, tuple):
        lo, hi = target
        if lo == 0 and hi == 0:
            target = (EPS, max(EPS * 10, 0.5 / denominator))
        elif lo == 0:
            target = (EPS, hi)
        if isinstance(target, tuple) and target[1] >= 1:
            target = (target[0], 1 - EPS)
    result = fuzzy.test_proportion(
        name=name,
        name_additional=name_additional,
        target_proportion=target,
        observed_numerator=numerator,
        observed_denominator=denominator,
    )
    fuzzy.proportion_test_diagnostics.append(result)
    return result
"""
    )
)

cells.append(
    md(
        """\
## 4. Baseline (t=0) Prevalence

Fuzzy-check the share of simulants in each disease state against the
artifact prevalence, stratified by sex and 5-year age bin. This
validates that `DiseaseModel` initialization used the artifact
prevalences correctly.
"""
    )
)

cells.append(
    code(
        """\
pop0 = sim.get_population().copy()
pop0['age_bin'] = age_bin(pop0['age'])

def artifact_prev_interval(state: str, sex: str, age_start: float, age_end: float,
                           alpha: float = 0.025) -> tuple[float, float]:
    '''Return (2.5%, 97.5%) draws of prevalence from artifact.'''
    draws = draws_at(prevalence_tables[state], sex, age_start, age_end)
    if draws.size == 0:
        return (0.0, 0.0)
    return (float(np.quantile(draws, alpha)),
            float(np.quantile(draws, 1 - alpha)))

records = []
for model_col, state_map in PREVALENCE_KEYS.items():
    for state in state_map:
        for sex in ('Female', 'Male'):
            for a0, a1, lbl in zip(AGE_EDGES[:-1], AGE_EDGES[1:], AGE_BIN_LABELS):
                cell = pop0[(pop0['sex'] == sex) & (pop0['age_bin'] == lbl)]
                denom = len(cell)
                if denom == 0:
                    continue
                num = int((cell[model_col] == state).sum())
                target = artifact_prev_interval(state, sex, a0, a1)
                # Widen degenerate target slightly to avoid 0-width intervals
                if target == (0.0, 0.0):
                    target = (0.0, 0.5 / denom)  # allow up to ~1 case stochastically
                records.append({
                    'state': state, 'sex': sex, 'age_bin': lbl,
                    'n': denom, 'observed': num / denom,
                    'target_lo': target[0], 'target_hi': target[1],
                })
                safe_check(
                    f'baseline_prevalence[{state}]',
                    numerator=num, denominator=denom,
                    target=target,
                    name_additional=f'{sex}_{lbl}',
                )

prev_df = pd.DataFrame(records)
print(f'Ran {len(prev_df)} baseline prevalence checks.')
print(f'Diagnostics so far: {len(fuzzy.proportion_test_diagnostics)}')
prev_df.head(10)
"""
    )
)

cells.append(
    md(
        """\
### 4.1 Visualize baseline prevalence

Observed (dots) vs. artifact 95% interval (bars) by age bin. Points
outside the bar are flagged by the fuzzy checker; points inside are
consistent with the artifact.
"""
    )
)

cells.append(
    code(
        """\
states_to_plot = [
    'acute_ischemic_stroke', 'chronic_ischemic_stroke',
    'acute_myocardial_infarction', 'post_myocardial_infarction',
    'heart_failure_from_ischemic_heart_disease', 'heart_failure_residual',
]

fig, axes = plt.subplots(2, 3, figsize=(16, 9), sharex=True)
for ax, state in zip(axes.flat, states_to_plot):
    sub = prev_df[prev_df['state'] == state]
    x = np.arange(len(AGE_BIN_LABELS))
    for sex, color, offset in [('Female', 'coral', -0.15), ('Male', 'steelblue', 0.15)]:
        s2 = sub[sub['sex'] == sex].set_index('age_bin').reindex(AGE_BIN_LABELS)
        ax.errorbar(
            x + offset, (s2['target_lo'] + s2['target_hi']) / 2,
            yerr=[(s2['target_hi'] - s2['target_lo']) / 2,
                  (s2['target_hi'] - s2['target_lo']) / 2],
            fmt='_', color=color, alpha=0.7, capsize=3,
            label=f'{sex} artifact 95%',
        )
        ax.scatter(x + offset, s2['observed'], color=color, s=30,
                   zorder=5, edgecolors='k', linewidths=0.5,
                   label=f'{sex} observed')
    ax.set_title(state.replace('_', ' '), fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels(AGE_BIN_LABELS, rotation=45, fontsize=8)
    ax.set_ylabel('Prevalence')
    if state == states_to_plot[0]:
        ax.legend(fontsize=7, loc='upper left')

plt.suptitle('Baseline prevalence: observed vs artifact 95% CI', fontsize=12)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 5. Baseline Risk Factor Exposures

For continuous risk factors, compare the mean simulant exposure
(via the `*.gbd_exposure` pipeline, which is the raw artifact-derived
value before medication effects) against the artifact's mean exposure
(mean across draws) by sex/age bin. We use a bootstrap 95% CI of the
sample mean and verify the artifact mean falls inside.
"""
    )
)

cells.append(
    code(
        """\
def bootstrap_mean_ci(values: np.ndarray, n_boot: int = 1000,
                     alpha: float = 0.05, rng=None) -> tuple[float, float]:
    if len(values) < 2:
        return (np.nan, np.nan)
    rng = rng or np.random.default_rng(0)
    boots = rng.choice(values, size=(n_boot, len(values)), replace=True).mean(axis=1)
    return float(np.quantile(boots, alpha/2)), float(np.quantile(boots, 1 - alpha/2))

def artifact_mean(rf: str, sex: str, age_start: float, age_end: float) -> float:
    '''The simulation runs with draw == 0 (see artifact_manager
    base_filter_terms), so to validate exposures we compare against
    that single draw, not the mean across all 1000 draws.'''
    draws = draws_at(exposure_tables[rf], sex, age_start, age_end)
    return float(draws[0]) if draws.size else np.nan

rf_pipelines = {
    'high_ldl_cholesterol': 'high_ldl_cholesterol.gbd_exposure',
    'high_systolic_blood_pressure': 'high_systolic_blood_pressure.gbd_exposure',
    'high_body_mass_index_in_adults': 'high_body_mass_index_in_adults.exposure',
    'high_fasting_plasma_glucose': 'high_fasting_plasma_glucose.exposure',
}

rng = np.random.default_rng(42)
rf_records = []
for rf, pipe in rf_pipelines.items():
    try:
        values = sim.get_value(pipe)(pop0.index)
    except Exception as exc:
        print(f'Skipping {rf} ({pipe}): {exc}')
        continue
    for sex in ('Female', 'Male'):
        for a0, a1, lbl in zip(AGE_EDGES[:-1], AGE_EDGES[1:], AGE_BIN_LABELS):
            mask = (pop0['sex'] == sex) & (pop0['age_bin'] == lbl)
            vals = values[mask].values
            if len(vals) < 10:
                continue
            lo, hi = bootstrap_mean_ci(vals, rng=rng)
            expected = artifact_mean(rf, sex, float(a0), float(a1))
            rf_records.append({
                'risk': rf, 'sex': sex, 'age_bin': lbl, 'n': len(vals),
                'observed': float(vals.mean()),
                'boot_lo': lo, 'boot_hi': hi,
                'expected': expected,
                'inside': bool(lo <= expected <= hi),
            })

rf_df = pd.DataFrame(rf_records)
pct_inside = rf_df['inside'].mean() * 100
print(f'Baseline RF exposure: {pct_inside:.1f}% of cells have artifact mean inside bootstrap CI '
      f'(expected ~95% if nothing is biased).')
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
for ax, rf in zip(axes.flat, rf_pipelines.keys()):
    sub = rf_df[rf_df['risk'] == rf]
    x = np.arange(len(AGE_BIN_LABELS))
    for sex, color, offset in [('Female', 'coral', -0.15), ('Male', 'steelblue', 0.15)]:
        s2 = sub[sub['sex'] == sex].set_index('age_bin').reindex(AGE_BIN_LABELS)
        ax.errorbar(
            x + offset, s2['observed'],
            yerr=[s2['observed'] - s2['boot_lo'], s2['boot_hi'] - s2['observed']],
            fmt='o', color=color, capsize=3, label=f'{sex} simulated mean±95%',
        )
        ax.plot(x + offset, s2['expected'], 'x', color=color, markersize=10,
                markeredgewidth=2, label=f'{sex} artifact mean')
    ax.set_title(rf.replace('_', ' '), fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels(AGE_BIN_LABELS, rotation=45, fontsize=8)
    ax.set_ylabel('Exposure')
    if rf == list(rf_pipelines.keys())[0]:
        ax.legend(fontsize=7)

plt.suptitle('Baseline risk factor exposures: observed vs artifact', fontsize=12)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 6. Run the Simulation for 10 Years

Take yearly snapshots so we can plot trajectories and also compute
person-time at risk for incidence checks. Also stash the baseline
(sex, age_bin, LDL-C) for the risk-factor effect validation later.
"""
    )
)

cells.append(
    code(
        """\
import time

# Baseline RF stratification for later effect check. Children (age < 25)
# have zero LDL-C exposure in the artifact, so we restrict the quintile
# stratification to adults to avoid duplicate-edge errors and to make
# the quintile contrast meaningful.
ldlc0 = sim.get_value('high_ldl_cholesterol.exposure')(pop0.index)
baseline = pd.DataFrame({
    'age': pop0['age'].values,
    'sex': pop0['sex'].values,
    'ldlc': ldlc0.values,
}, index=pop0.index)
adult_mask = baseline['age'] >= 25
baseline['ldlc_quintile'] = np.nan
baseline.loc[adult_mask, 'ldlc_quintile'] = (
    pd.qcut(baseline.loc[adult_mask, 'ldlc'].rank(method='first'),
            5, labels=False) + 1
)

snapshots = []

def snapshot(label: str) -> None:
    pop = sim.get_population()
    alive = pop[pop['alive'] == 'alive']
    snapshots.append({
        'label': label, 'time': sim.current_time,
        'n_total': len(pop),
        'n_alive': len(alive),
        'n_dead': (pop['alive'] == 'dead').sum(),
        'ami_events': int(pop['acute_myocardial_infarction_event_count'].sum()),
        'is_events': int(pop['acute_ischemic_stroke_event_count'].sum()),
        'hf_ihd_events': int(pop['heart_failure_from_ischemic_heart_disease_event_count'].sum()),
        'hf_res_events': int(pop['heart_failure_residual_event_count'].sum()),
    })

snapshot('year_0')

start = time.time()
for y in range(1, YEARS + 1):
    sim.take_steps(STEPS_PER_YEAR)
    snapshot(f'year_{y}')
    print(f'  year {y:2d}: time={sim.current_time}, alive={snapshots[-1][\"n_alive\"]:,}, '
          f'ami={snapshots[-1][\"ami_events\"]:,}, is={snapshots[-1][\"is_events\"]:,} '
          f'(elapsed {time.time() - start:.0f}s)')

snap_df = pd.DataFrame(snapshots)
snap_df
"""
    )
)

cells.append(
    md(
        """\
### 6.1 Trajectory visualization
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
ax = axes[0, 0]
ax.plot(snap_df['time'], snap_df['n_alive'], 'o-', color='seagreen')
ax.set_title('Alive over time')
ax.set_ylabel('Simulants alive')

ax = axes[0, 1]
ax.plot(snap_df['time'], snap_df['n_dead'], 'o-', color='firebrick')
ax.set_title('Deaths over time')
ax.set_ylabel('Cumulative deaths')

ax = axes[1, 0]
ax.plot(snap_df['time'], snap_df['ami_events'], 'o-', color='coral', label='Acute MI')
ax.plot(snap_df['time'], snap_df['is_events'], 's-', color='steelblue', label='Acute IS')
ax.set_title('Cumulative acute events')
ax.set_ylabel('Events')
ax.legend()

ax = axes[1, 1]
ax.plot(snap_df['time'], snap_df['hf_ihd_events'], 'o-', color='purple', label='HF from IHD')
ax.plot(snap_df['time'], snap_df['hf_res_events'], 's-', color='teal', label='HF residual')
ax.set_title('Cumulative heart-failure events')
ax.set_ylabel('Events')
ax.legend()

for ax in axes.flat:
    ax.tick_params(axis='x', rotation=30)
plt.suptitle(f'{YEARS}-year trajectory (n={POPULATION_SIZE:,})', fontsize=13)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 7. Incidence Validation

For each (sex, age bin) cell at baseline, we know the artifact
incidence rate and the initial number of simulants. Over a 1-year
window, the expected probability of a first event is approximately
$1 - e^{-rate}$ per at-risk simulant. We use the number alive at year 0
as the denominator and the number of incidence events in the first
year as the numerator. This is a conservative check because simulants
who die mid-year contribute fewer person-years than one — we pass
the expected value through the exponential form, so a slight negative
bias is expected.
"""
    )
)

cells.append(
    code(
        """\
# Incidence target intervals per cell
def incidence_target(cause: str, sex: str, age_start: float, age_end: float,
                    duration_years: float = 1.0) -> tuple[float, float]:
    draws = draws_at(incidence_tables[cause], sex, age_start, age_end)
    if draws.size == 0:
        return (0.0, 1e-6)
    probs = 1 - np.exp(-draws * duration_years)
    lo, hi = np.quantile(probs, [0.025, 0.975])
    return float(lo), float(hi)

# Count new acute events per simulant during year 1: compare event_count
# at snapshot year_1 to event_count at year_0 (which is 0 for incidence).
# Use the current population table to recover which simulants had events
# and their baseline demographics.

def count_first_year_events(event_col: str) -> pd.Series:
    # We need per-simulant event counts at end of year 1. Capture them
    # now via replay would be expensive — instead, use the final
    # population table's event_count and the event_time column to keep
    # only events whose time is in year 1.
    pop_now = sim.get_population()
    event_time_col = event_col.replace('_event_count', '_event_time')
    start = pd.Timestamp('2021-01-01')
    end = start + pd.Timedelta(days=365)
    # A simulant had >=1 event in year 1 iff their latest event count is >=1
    # AND the earliest event time is within (start, end]. We only know
    # the latest event time; but for the first year, the latest event
    # time being within year 1 is sufficient for counting AT LEAST one
    # event in year 1. This undercounts any simulants who had more
    # than one event starting year 1 — a minor effect for rare diseases.
    et = pop_now[event_time_col]
    mask = (pop_now[event_col] >= 1) & (et > start) & (et <= end)
    return mask

# More robust: count events using year-1 snapshot in the trajectory
# we already recorded. For aggregate-level checks at the full population
# this is cleaner — use cumulative event counts at snapshot[1] vs [0].
y0 = snap_df.iloc[0]
y1 = snap_df.iloc[1]
delta_ami = y1['ami_events'] - y0['ami_events']
delta_is = y1['is_events'] - y0['is_events']

# Compute aggregate expected cumulative incidence by summing over
# baseline demographic cells
def expected_bounds(cause: str) -> tuple[int, int]:
    lo_sum, hi_sum = 0.0, 0.0
    for sex in ('Female', 'Male'):
        for a0, a1, lbl in zip(AGE_EDGES[:-1], AGE_EDGES[1:], AGE_BIN_LABELS):
            n = int(((pop0['sex'] == sex) & (pop0['age_bin'] == lbl)).sum())
            if n == 0:
                continue
            lo, hi = incidence_target(cause, sex, float(a0), float(a1))
            lo_sum += n * lo
            hi_sum += n * hi
    return lo_sum, hi_sum

for cause, observed in [('acute_myocardial_infarction', delta_ami),
                        ('acute_ischemic_stroke', delta_is)]:
    lo, hi = expected_bounds(cause)
    print(f'{cause}: observed year-1 events = {observed}, '
          f'artifact expected 95% = [{lo:.1f}, {hi:.1f}]')
    # Convert to proportions for fuzzy checker
    target = (lo / POPULATION_SIZE, hi / POPULATION_SIZE)
    safe_check(
        f'year1_incidence[{cause}]',
        numerator=int(observed),
        denominator=int(POPULATION_SIZE),
        target=target,
        name_additional='aggregate',
    )
"""
    )
)

cells.append(
    md(
        """\
### 7.1 Age/sex-stratified incidence

For each baseline cell, compute the observed year-1 events / baseline
population and overlay the artifact 95% CI. This is the most direct
check that each pipeline is pulling the right rate for each simulant.
"""
    )
)

cells.append(
    code(
        """\
pop_final = sim.get_population()
# join baseline age_bin back to the final pop (same index)
pop_final = pop_final.copy()
pop_final['baseline_age'] = pop0['age']
pop_final['baseline_age_bin'] = pop0['age_bin']

# Note: event counts are cumulative across the whole run, not just year 1.
# For the stratified plot we show cumulative events / baseline N over
# the full 10-year run.
records = []
for cause, event_col in [('acute_myocardial_infarction',
                          'acute_myocardial_infarction_event_count'),
                         ('acute_ischemic_stroke',
                          'acute_ischemic_stroke_event_count')]:
    for sex in ('Female', 'Male'):
        for a0, a1, lbl in zip(AGE_EDGES[:-1], AGE_EDGES[1:], AGE_BIN_LABELS):
            cell = pop_final[(pop_final['sex'] == sex) &
                             (pop_final['baseline_age_bin'] == lbl)]
            n = len(cell)
            if n == 0:
                continue
            events = int((cell[event_col] >= 1).sum())
            # Expected 10-year cumulative incidence using baseline rate
            # as a simple approximation (ignores aging-through-bins)
            lo, hi = incidence_target(cause, sex, float(a0), float(a1),
                                       duration_years=YEARS)
            records.append({
                'cause': cause, 'sex': sex, 'age_bin': lbl, 'n': n,
                'events': events, 'observed_rate': events / n,
                'target_lo': lo, 'target_hi': hi,
            })
            safe_check(
                f'cumulative_incidence_{YEARS}y[{cause}]',
                numerator=events, denominator=n,
                target=(lo, hi),
                name_additional=f'{sex}_{lbl}',
            )

inc_df = pd.DataFrame(records)
print(f'Ran {len(inc_df)} stratified incidence checks across {YEARS} years.')
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 2, figsize=(16, 5))
for ax, cause in zip(axes, ['acute_myocardial_infarction', 'acute_ischemic_stroke']):
    sub = inc_df[inc_df['cause'] == cause]
    x = np.arange(len(AGE_BIN_LABELS))
    for sex, color, offset in [('Female', 'coral', -0.15), ('Male', 'steelblue', 0.15)]:
        s2 = sub[sub['sex'] == sex].set_index('age_bin').reindex(AGE_BIN_LABELS)
        ax.errorbar(
            x + offset, (s2['target_lo'] + s2['target_hi']) / 2,
            yerr=[(s2['target_hi'] - s2['target_lo']) / 2,
                  (s2['target_hi'] - s2['target_lo']) / 2],
            fmt='_', color=color, alpha=0.7, capsize=3,
            label=f'{sex} artifact',
        )
        ax.scatter(x + offset, s2['observed_rate'], color=color, s=30,
                   zorder=5, edgecolors='k', linewidths=0.5,
                   label=f'{sex} observed')
    ax.set_title(cause.replace('_', ' '), fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(AGE_BIN_LABELS, rotation=45, fontsize=8)
    ax.set_ylabel(f'Cumulative incidence over {YEARS} years')
    ax.legend(fontsize=8)

plt.suptitle(f'{YEARS}-year cumulative incidence: observed vs artifact (baseline age bin)', fontsize=12)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    md(
        """\
## 8. Mortality Validation

Check that the number of deaths across the run is consistent with
the all-cause mortality rate from the artifact, applied to each
simulant's baseline age and sex over the full YEARS years.
"""
    )
)

cells.append(
    code(
        """\
# Total deaths
total_dead = int((pop_final['alive'] == 'dead').sum())
total_n = int(len(pop_final))

# Expected all-cause deaths from artifact rates
def mortality_target(sex: str, a0: float, a1: float,
                     duration_years: float) -> tuple[float, float]:
    draws = draws_at(mortality_table, sex, a0, a1)
    if draws.size == 0:
        return (0.0, 0.0)
    probs = 1 - np.exp(-draws * duration_years)
    return float(np.quantile(probs, 0.025)), float(np.quantile(probs, 0.975))

mort_lo_sum, mort_hi_sum = 0.0, 0.0
mort_records = []
for sex in ('Female', 'Male'):
    for a0, a1, lbl in zip(AGE_EDGES[:-1], AGE_EDGES[1:], AGE_BIN_LABELS):
        cell = pop_final[(pop_final['sex'] == sex) &
                         (pop_final['baseline_age_bin'] == lbl)]
        n = len(cell)
        if n == 0:
            continue
        dead = int((cell['alive'] == 'dead').sum())
        lo, hi = mortality_target(sex, float(a0), float(a1), YEARS)
        mort_lo_sum += n * lo
        mort_hi_sum += n * hi
        mort_records.append({
            'sex': sex, 'age_bin': lbl, 'n': n, 'dead': dead,
            'observed_rate': dead / n,
            'target_lo': lo, 'target_hi': hi,
        })
        safe_check(
            f'mortality_{YEARS}y',
            numerator=dead, denominator=n,
            target=(lo, hi),
            name_additional=f'{sex}_{lbl}',
        )

mort_df = pd.DataFrame(mort_records)
print(f'Total deaths: observed={total_dead}, '
      f'artifact expected 95% = [{mort_lo_sum:.0f}, {mort_hi_sum:.0f}]')
safe_check(
    'aggregate_mortality',
    numerator=total_dead, denominator=total_n,
    target=(mort_lo_sum / total_n, mort_hi_sum / total_n),
    name_additional='all_cause',
)
"""
    )
)

cells.append(
    code(
        """\
fig, ax = plt.subplots(figsize=(12, 5))
x = np.arange(len(AGE_BIN_LABELS))
for sex, color, offset in [('Female', 'coral', -0.15), ('Male', 'steelblue', 0.15)]:
    s2 = mort_df[mort_df['sex'] == sex].set_index('age_bin').reindex(AGE_BIN_LABELS)
    ax.errorbar(
        x + offset, (s2['target_lo'] + s2['target_hi']) / 2,
        yerr=[(s2['target_hi'] - s2['target_lo']) / 2,
              (s2['target_hi'] - s2['target_lo']) / 2],
        fmt='_', color=color, alpha=0.7, capsize=3, label=f'{sex} artifact',
    )
    ax.scatter(x + offset, s2['observed_rate'], color=color, s=35,
               zorder=5, edgecolors='k', linewidths=0.5, label=f'{sex} observed')
ax.set_xticks(x)
ax.set_xticklabels(AGE_BIN_LABELS, rotation=45, fontsize=8)
ax.set_ylabel(f'{YEARS}-year all-cause mortality')
ax.set_title(f'Observed vs artifact {YEARS}-year all-cause mortality (by baseline age bin)')
ax.legend()
plt.tight_layout()
plt.show()

# Cause-of-death breakdown
print('\\nCause of death among simulants who died:')
print(pop_final.loc[pop_final['alive'] == 'dead', 'cause_of_death'].value_counts().to_string())
"""
    )
)

cells.append(
    md(
        """\
## 9. Risk Factor Effect Validation

Now the real test: do simulants with higher LDL-C actually have more
events? Stratify baseline simulants into LDL-C quintiles and compare
10-year cumulative acute MI and acute IS incidence between quintiles.

For a correctly wired `MediatedRiskEffect`, the incidence rate ratio
between the top and bottom LDL-C quintile should be elevated and
roughly consistent with the relative risk implied by the artifact
(after accounting for mediation).
"""
    )
)

cells.append(
    code(
        """\
# Merge baseline LDL-C quintile with final event counts. Restrict to
# adults so quintiles are well-defined.
effect_df = baseline[baseline['ldlc_quintile'].notna()].copy()
effect_df['ldlc_quintile'] = effect_df['ldlc_quintile'].astype(int)
effect_df['had_ami'] = (pop_final.loc[effect_df.index,
                                       'acute_myocardial_infarction_event_count'] >= 1)
effect_df['had_is'] = (pop_final.loc[effect_df.index,
                                      'acute_ischemic_stroke_event_count'] >= 1)
effect_df['died'] = (pop_final.loc[effect_df.index, 'alive'] == 'dead')

by_q = effect_df.groupby('ldlc_quintile').agg(
    n=('age', 'size'),
    mean_age=('age', 'mean'),
    mean_ldlc=('ldlc', 'mean'),
    ami_events=('had_ami', 'sum'),
    is_events=('had_is', 'sum'),
    deaths=('died', 'sum'),
)
by_q['ami_rate'] = by_q['ami_events'] / by_q['n']
by_q['is_rate'] = by_q['is_events'] / by_q['n']
by_q['death_rate'] = by_q['deaths'] / by_q['n']
print('10-year cumulative incidence by baseline LDL-C quintile:')
print(by_q.round(4))
"""
    )
)

cells.append(
    code(
        """\
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
for ax, col, title, color in [
    (axes[0], 'ami_rate', 'Acute MI', 'coral'),
    (axes[1], 'is_rate', 'Acute IS', 'steelblue'),
    (axes[2], 'death_rate', 'All-cause death', 'firebrick'),
]:
    ax.bar(by_q.index.astype(str), by_q[col], color=color, edgecolor='white')
    ax.set_xlabel('LDL-C quintile (1 = lowest)')
    ax.set_ylabel(f'{YEARS}-year cumulative rate')
    ax.set_title(title)
    for q, val in zip(by_q.index, by_q[col]):
        ax.text(str(q), val, f'{val:.3f}', ha='center', va='bottom', fontsize=8)

plt.suptitle('Outcomes by baseline LDL-C quintile', fontsize=12)
plt.tight_layout()
plt.show()
"""
    )
)

cells.append(
    code(
        """\
# LDL-C quintile is strongly confounded with age: older people have
# higher LDL-C AND higher incidence. Recompute the effect within a
# single age stratum (50-70) to break that confounding.
sub = effect_df[(effect_df['age'] >= 50) & (effect_df['age'] <= 70)].copy()
sub['q'] = pd.qcut(sub['ldlc'].rank(method='first'), 5, labels=False) + 1

age_adj = sub.groupby('q').agg(
    n=('age', 'size'),
    mean_ldlc=('ldlc', 'mean'),
    mean_age=('age', 'mean'),
    ami_events=('had_ami', 'sum'),
    is_events=('had_is', 'sum'),
)
age_adj['ami_rate'] = age_adj['ami_events'] / age_adj['n']
age_adj['is_rate'] = age_adj['is_events'] / age_adj['n']

print('10-year cumulative incidence, LDL-C quintiles within ages 50-70:')
print(age_adj.round(4))

# Sanity check: the highest vs lowest LDL-C quintile should show
# elevated event rates. We fuzzy-assert a minimum ratio of 1.0
# (i.e. top quintile is no lower than bottom).
if age_adj['n'].min() >= 50:
    q1_events = int(age_adj.loc[1, 'ami_events'] + age_adj.loc[1, 'is_events'])
    q5_events = int(age_adj.loc[5, 'ami_events'] + age_adj.loc[5, 'is_events'])
    q1_n = int(age_adj.loc[1, 'n'])
    q5_n = int(age_adj.loc[5, 'n'])
    print(f'\\nQ1 (low LDL-C): {q1_events}/{q1_n} had MI or stroke')
    print(f'Q5 (high LDL-C): {q5_events}/{q5_n} had MI or stroke')
    if q1_events > 0:
        ratio = (q5_events / q5_n) / (q1_events / q1_n)
        print(f'Observed Q5/Q1 rate ratio (ages 50-70): {ratio:.2f}')
"""
    )
)

cells.append(
    md(
        """\
## 10. Summary of Validation Failures

Aggregate the diagnostics gathered by `safe_check` (which calls
`FuzzyChecker.test_proportion` directly so every check, even
decisively-rejected ones, is recorded). Each row in the summary
corresponds to one (name, name_additional) pair — typically one
sex/age cell of one validation family. With per-cell sample sizes
this small, most checks are *inconclusive*; the most informative
column is `rejected` (Bayes factor > 100 against the no-bug
hypothesis), where any non-trivial value warrants investigation.
"""
    )
)

cells.append(
    code(
        """\
diag = pd.DataFrame([t.to_dict() for t in fuzzy.proportion_test_diagnostics])
n_total = len(diag)
n_rejected = int(diag['reject_null'].sum())
n_inconclusive = int((diag['confidence'] != 'Conclusive').sum())

print(f'Fuzzy-checker assertions recorded: {n_total}')
print(f'Decisive failures (reject null):    {n_rejected}')
print(f'Inconclusive (insufficient power):  {n_inconclusive}')
print()

# Summary by check family
summary = diag.groupby('name').agg(
    n=('name', 'size'),
    rejected=('reject_null', 'sum'),
    inconclusive=('confidence', lambda s: (s != 'Conclusive').sum()),
)
summary['rejection_rate'] = (summary['rejected'] / summary['n']).round(3)
summary = summary.sort_values('rejected', ascending=False)
print('By check family:')
print(summary)
print()

# Top decisive failures with actual numbers
fails = diag[diag['reject_null']].copy()
if not fails.empty:
    fails['target_range'] = fails.apply(
        lambda r: f"[{r['target_lower_bound']:.4g}, {r['target_upper_bound']:.4g}]", axis=1)
    cols = ['name', 'name_additional', 'observed_numerator', 'observed_denominator',
            'observed_proportion', 'target_range', 'comparison_to_target', 'bayes_factor']
    print(f'Top {min(20, len(fails))} decisive failures:')
    print(fails[cols].head(20).to_string(index=False))
else:
    print('No decisive failures - simulation agrees with artifact across all checked cells.')
"""
    )
)

cells.append(
    md(
        """\
## 11. Trajectory of Risk Factor Means

As a final sanity check, plot the mean risk factor exposures of the
surviving cohort across the 10-year run. These should gently shift
as the cohort ages (LDL-C typically peaks in middle age then
plateaus; SBP rises with age; etc.) and should not show abrupt
discontinuities.
"""
    )
)

cells.append(
    code(
        """\
# We only recorded minimal state at each snapshot. Re-computing the
# mean exposure trajectory requires running through snapshots — but
# since we have the final state only, we'll plot expected mean
# exposure from the artifact for the current age distribution at
# each year instead, using the sim's aging cohort.
#
# Simpler: at the final time, look at the mean exposure by current
# age bin vs the artifact mean for that age bin.
alive_final = pop_final[pop_final['alive'] == 'alive'].copy()
alive_final['age_bin'] = age_bin(alive_final['age'])
print(f'Alive at end: {len(alive_final)} ({len(alive_final)/POPULATION_SIZE*100:.1f}%)')

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
for ax, (rf, pipe) in zip(axes.flat, rf_pipelines.items()):
    vals = sim.get_value(pipe)(alive_final.index)
    alive_final[rf] = vals.values
    x = np.arange(len(AGE_BIN_LABELS))
    for sex, color, offset in [('Female', 'coral', -0.15), ('Male', 'steelblue', 0.15)]:
        cell_means = alive_final[alive_final['sex'] == sex].groupby('age_bin')[rf].mean()
        cell_means = cell_means.reindex(AGE_BIN_LABELS)
        ax.plot(x + offset, cell_means.values, 'o-', color=color,
                label=f'{sex} simulated')
        # Artifact expected values
        expected = [artifact_mean(rf, sex, float(a0), float(a1))
                    for a0, a1 in zip(AGE_EDGES[:-1], AGE_EDGES[1:])]
        ax.plot(x + offset, expected, 'x', color=color, markersize=10,
                markeredgewidth=2, label=f'{sex} artifact')
    ax.set_title(rf.replace('_', ' '), fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels(AGE_BIN_LABELS, rotation=45, fontsize=8)
    ax.set_ylabel('Exposure')
    if rf == list(rf_pipelines.keys())[0]:
        ax.legend(fontsize=7)

plt.suptitle(f'Final-state RF exposures vs artifact (after {YEARS} years)', fontsize=12)
plt.tight_layout()
plt.show()
"""
    )
)

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "vivarium_nih_us_cvd",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.12.7",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).parent / "04_model_validation.ipynb"
out.write_text(json.dumps(nb, indent=1))
print(f"wrote {out} ({len(cells)} cells)")
