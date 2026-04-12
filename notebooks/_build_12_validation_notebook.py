"""Build notebook 12: Old-vs-new validation (GBD 2020 Alabama vs GBD 2023 USA)."""
import nbformat as nbf

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {
    "display_name": "Python 3",
    "language": "python",
    "name": "python3",
}

cells = []

# --- Title ---
cells.append(nbf.v4.new_markdown_cell(
    "# Validation: GBD 2020 Alabama vs GBD 2023 USA\n"
    "\n"
    "This notebook compares the old artifact (GBD 2020, Alabama, 1000 draws) "
    "with the new artifact (GBD 2023, USA national, 250 draws) and runs the "
    "production model to verify correctness.\n"
    "\n"
    "**Expected differences:**\n"
    "- Different location (state vs national) -> different absolute rates\n"
    "- Different GBD round -> updated estimates\n"
    "- Age bins: 23 -> 25 (added age groups)\n"
    "- Draw count: 1000 -> 250\n"
    "- RR format: SBP/LDL-C/FPG switched from log-linear (184 rows) to "
    "non-log-linear (200K rows)\n"
    "\n"
    "**Things that should be similar:**\n"
    "- Mediation factors (location-independent)\n"
    "- Exposure distribution weight patterns\n"
    "- General magnitude of rates and exposures"
))

# --- Setup ---
cells.append(nbf.v4.new_code_cell(
    "import pandas as pd\n"
    "import numpy as np\n"
    "\n"
    "OLD_PATH = '../src/vivarium_nih_us_cvd/artifacts/alabama.hdf'\n"
    "NEW_PATH = '../src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf'\n"
    "\n"
    "old_store = pd.HDFStore(OLD_PATH, 'r')\n"
    "new_store = pd.HDFStore(NEW_PATH, 'r')\n"
    "\n"
    "print(f'Old artifact keys: {len(old_store.keys())}')\n"
    "print(f'New artifact keys: {len(new_store.keys())}')\n"
    "print(f'Keys match: {set(old_store.keys()) == set(new_store.keys())}')"
))

# ==========================================
# Section 1: Demographic structure
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 1. Demographic Structure\n"
    "\n"
    "Compare age bins, population structure, and demographic dimensions."
))

cells.append(nbf.v4.new_code_cell(
    "# Age bins\n"
    "old_bins = old_store['/population/age_bins']\n"
    "new_bins = new_store['/population/age_bins']\n"
    "\n"
    "print(f'Age bins: old={len(old_bins)}, new={len(new_bins)}')\n"
    "print()\n"
    "\n"
    "# Find new age groups\n"
    "old_names = set(old_bins['age_group_name'])\n"
    "new_names = set(new_bins['age_group_name'])\n"
    "print('Age groups only in NEW:', new_names - old_names)\n"
    "print('Age groups only in OLD:', old_names - new_names)\n"
    "print()\n"
    "\n"
    "# Population structure (draw_0 values)\n"
    "old_pop = old_store['/population/structure']\n"
    "new_pop = new_store['/population/structure']\n"
    "old_pop.index = old_pop.index.droplevel([c for c in old_pop.index.names if c not in ['age_start', 'age_end', 'sex']])\n"
    "new_pop.index = new_pop.index.droplevel([c for c in new_pop.index.names if c not in ['age_start', 'age_end', 'sex']])\n"
    "\n"
    "# Compare common age groups\n"
    "common = old_pop.index.intersection(new_pop.index)\n"
    "print(f'Common demographic bins: {len(common)}')\n"
    "print(f'Old-only demographic bins: {len(old_pop.index.difference(new_pop.index))}')\n"
    "print(f'New-only demographic bins: {len(new_pop.index.difference(old_pop.index))}')"
))

# ==========================================
# Section 2: Risk exposure distributions
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 2. Risk Exposure Distributions\n"
    "\n"
    "Compare mean exposure levels for SBP, LDL-C, BMI, and FPG across "
    "age and sex. We compare draw_0 values (since draw counts differ)."
))

cells.append(nbf.v4.new_code_cell(
    "risks = [\n"
    "    ('SBP', 'high_systolic_blood_pressure'),\n"
    "    ('LDL-C', 'high_ldl_cholesterol'),\n"
    "    ('BMI', 'high_body_mass_index_in_adults'),\n"
    "    ('FPG', 'high_fasting_plasma_glucose'),\n"
    "]\n"
    "\n"
    "for label, risk in risks:\n"
    "    old_exp = old_store[f'/risk_factor/{risk}/exposure']\n"
    "    new_exp = new_store[f'/risk_factor/{risk}/exposure']\n"
    "    \n"
    "    # Use draw_0 for comparison\n"
    "    old_vals = old_exp['draw_0'].values\n"
    "    new_vals = new_exp['draw_0'].values\n"
    "    \n"
    "    print(f'{label}:')\n"
    "    print(f'  Old (Alabama): mean={old_vals.mean():.2f}, '\n"
    "          f'range=[{old_vals.min():.2f}, {old_vals.max():.2f}], '\n"
    "          f'rows={len(old_exp)}')\n"
    "    print(f'  New (USA):     mean={new_vals.mean():.2f}, '\n"
    "          f'range=[{new_vals.min():.2f}, {new_vals.max():.2f}], '\n"
    "          f'rows={len(new_exp)}')\n"
    "    print()"
))

cells.append(nbf.v4.new_code_cell(
    "# Detailed exposure comparison by age group (ages 25+)\n"
    "for label, risk in risks:\n"
    "    old_exp = old_store[f'/risk_factor/{risk}/exposure']\n"
    "    new_exp = new_store[f'/risk_factor/{risk}/exposure']\n"
    "    \n"
    "    # Reset index to get age/sex as columns\n"
    "    old_df = old_exp.reset_index()\n"
    "    new_df = new_exp.reset_index()\n"
    "    \n"
    "    # Keep only adult age groups (25+)\n"
    "    old_df = old_df[old_df['age_start'] >= 25]\n"
    "    new_df = new_df[new_df['age_start'] >= 25]\n"
    "    \n"
    "    # Merge on common age/sex bins\n"
    "    merge_cols = ['age_start', 'age_end', 'sex']\n"
    "    merged = old_df[merge_cols + ['draw_0']].merge(\n"
    "        new_df[merge_cols + ['draw_0']],\n"
    "        on=merge_cols, suffixes=('_old', '_new'),\n"
    "        how='inner'\n"
    "    )\n"
    "    \n"
    "    merged['pct_diff'] = 100 * (merged['draw_0_new'] - merged['draw_0_old']) / merged['draw_0_old']\n"
    "    \n"
    "    print(f'\\n{label} — draw_0 comparison (ages 25+):')\n"
    "    print(f'  Mean pct change: {merged[\"pct_diff\"].mean():.1f}%')\n"
    "    print(f'  Max abs pct change: {merged[\"pct_diff\"].abs().max():.1f}%')\n"
    "    \n"
    "    # Show largest differences\n"
    "    biggest = merged.nlargest(3, 'pct_diff')[merge_cols + ['draw_0_old', 'draw_0_new', 'pct_diff']]\n"
    "    smallest = merged.nsmallest(3, 'pct_diff')[merge_cols + ['draw_0_old', 'draw_0_new', 'pct_diff']]\n"
    "    print('  Largest increases:')\n"
    "    for _, row in biggest.iterrows():\n"
    "        pct = row['pct_diff']\n"
    "        print(f'    age {row[\"age_start\"]:.0f}-{row[\"age_end\"]:.0f} {row[\"sex\"]}: '\n"
    "              f'{row[\"draw_0_old\"]:.2f} -> {row[\"draw_0_new\"]:.2f} ({pct:+.1f}%)')\n"
    "    print('  Largest decreases:')\n"
    "    for _, row in smallest.iterrows():\n"
    "        pct = row['pct_diff']\n"
    "        print(f'    age {row[\"age_start\"]:.0f}-{row[\"age_end\"]:.0f} {row[\"sex\"]}: '\n"
    "              f'{row[\"draw_0_old\"]:.2f} -> {row[\"draw_0_new\"]:.2f} ({pct:+.1f}%)')"
))

# ==========================================
# Section 3: Exposure distribution weights
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 3. Exposure Distribution Weights\n"
    "\n"
    "These determine which parametric distribution best fits each risk's "
    "exposure in each age/sex bin. They should be structurally similar "
    "since distribution shapes are location-independent."
))

cells.append(nbf.v4.new_code_cell(
    "for label, risk in risks:\n"
    "    old_w = old_store[f'/risk_factor/{risk}/exposure_distribution_weights']\n"
    "    new_w = new_store[f'/risk_factor/{risk}/exposure_distribution_weights']\n"
    "    \n"
    "    print(f'{label}:')\n"
    "    print(f'  Old shape: {old_w.shape}, New shape: {new_w.shape}')\n"
    "    \n"
    "    # Check parameter (distribution type) values\n"
    "    old_reset = old_w.reset_index()\n"
    "    new_reset = new_w.reset_index()\n"
    "    \n"
    "    if 'parameter' in old_reset.columns:\n"
    "        old_dists = sorted(old_reset['parameter'].unique())\n"
    "        print(f'  Old distributions: {old_dists}')\n"
    "    if 'parameter' in new_reset.columns:\n"
    "        new_dists = sorted(new_reset['parameter'].unique())\n"
    "        print(f'  New distributions: {new_dists}')\n"
    "    print()"
))

# ==========================================
# Section 4: Disease rates
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 4. Disease Incidence and Mortality Rates\n"
    "\n"
    "Compare key cause rates between the two artifacts. "
    "Differences here reflect both the location change (Alabama vs USA) "
    "and the GBD round update."
))

cells.append(nbf.v4.new_code_cell(
    "rate_keys = [\n"
    "    ('IS incidence', '/cause/ischemic_stroke/incidence_rate'),\n"
    "    ('AMI incidence', '/cause/acute_myocardial_infarction/incidence_rate'),\n"
    "    ('HF-IHD incidence', '/cause/heart_failure_from_ischemic_heart_disease/incidence_rate'),\n"
    "    ('HF-residual incidence', '/cause/heart_failure_residual/incidence_rate'),\n"
    "    ('IS CSMR', '/cause/ischemic_stroke/cause_specific_mortality_rate'),\n"
    "    ('IHD+HF CSMR', '/cause/ischemic_heart_disease_and_heart_failure/cause_specific_mortality_rate'),\n"
    "    ('All-cause mortality', '/cause/all_causes/cause_specific_mortality_rate'),\n"
    "]\n"
    "\n"
    "for label, key in rate_keys:\n"
    "    old_df = old_store[key].reset_index()\n"
    "    new_df = new_store[key].reset_index()\n"
    "    \n"
    "    # Adults 25+\n"
    "    old_adult = old_df[old_df['age_start'] >= 25]\n"
    "    new_adult = new_df[new_df['age_start'] >= 25]\n"
    "    \n"
    "    old_mean = old_adult['draw_0'].mean()\n"
    "    new_mean = new_adult['draw_0'].mean()\n"
    "    pct = 100 * (new_mean - old_mean) / old_mean if old_mean != 0 else float('inf')\n"
    "    \n"
    "    print(f'{label:30s}  old={old_mean:.6f}  new={new_mean:.6f}  change={pct:+.1f}%')"
))

# ==========================================
# Section 5: Relative risks
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 5. Relative Risk Comparison\n"
    "\n"
    "The biggest structural change: SBP, LDL-C, and FPG relative risks "
    "switched from log-linear (per-unit RR, ~184 rows) to non-log-linear "
    "(exposure-level-specific RRs, ~200K rows). BMI remained log-linear.\n"
    "\n"
    "We can compare BMI RRs directly and examine the non-log-linear RR structure."
))

cells.append(nbf.v4.new_code_cell(
    "# BMI relative risk — still log-linear in both\n"
    "old_bmi_rr = old_store['/risk_factor/high_body_mass_index_in_adults/relative_risk'].reset_index()\n"
    "new_bmi_rr = new_store['/risk_factor/high_body_mass_index_in_adults/relative_risk'].reset_index()\n"
    "\n"
    "print('BMI Relative Risk')\n"
    "print(f'  Old shape: {old_bmi_rr.shape}')\n"
    "print(f'  New shape: {new_bmi_rr.shape}')\n"
    "print()\n"
    "\n"
    "# Check affected causes in old vs new\n"
    "if 'affected_entity' in old_bmi_rr.columns:\n"
    "    print('Old affected entities:', sorted(old_bmi_rr['affected_entity'].unique()))\n"
    "if 'affected_entity' in new_bmi_rr.columns:\n"
    "    print('New affected entities:', sorted(new_bmi_rr['affected_entity'].unique()))\n"
    "print()\n"
    "\n"
    "# Compare HF RRs (present in both)\n"
    "for entity in ['heart_failure_from_ischemic_heart_disease', 'heart_failure_residual']:\n"
    "    if 'affected_entity' in old_bmi_rr.columns and 'affected_entity' in new_bmi_rr.columns:\n"
    "        old_hf = old_bmi_rr[old_bmi_rr['affected_entity'] == entity]['draw_0']\n"
    "        new_hf = new_bmi_rr[new_bmi_rr['affected_entity'] == entity]['draw_0']\n"
    "        print(f'  BMI -> {entity}:')\n"
    "        print(f'    Old: mean RR={old_hf.mean():.4f}, range=[{old_hf.min():.4f}, {old_hf.max():.4f}]')\n"
    "        print(f'    New: mean RR={new_hf.mean():.4f}, range=[{new_hf.min():.4f}, {new_hf.max():.4f}]')"
))

cells.append(nbf.v4.new_code_cell(
    "# Non-log-linear RR structure for SBP (representative)\n"
    "new_sbp_rr = new_store['/risk_factor/high_systolic_blood_pressure/relative_risk'].reset_index()\n"
    "old_sbp_rr = old_store['/risk_factor/high_systolic_blood_pressure/relative_risk'].reset_index()\n"
    "\n"
    "print('SBP Relative Risk')\n"
    "print(f'  Old: {old_sbp_rr.shape} (log-linear, per-unit RR)')\n"
    "print(f'  New: {new_sbp_rr.shape} (non-log-linear, exposure-level RRs)')\n"
    "print()\n"
    "print('New SBP RR columns:', list(new_sbp_rr.columns[:10]), '...')\n"
    "print()\n"
    "\n"
    "if 'exposure' in new_sbp_rr.columns:\n"
    "    print('Exposure levels in new RR data:')\n"
    "    print(f'  min={new_sbp_rr[\"exposure\"].min():.1f}, max={new_sbp_rr[\"exposure\"].max():.1f}, '\n"
    "          f'unique={new_sbp_rr[\"exposure\"].nunique()}')\n"
    "    print()\n"
    "    # Show RR at selected exposure levels for one age/sex\n"
    "    sample = new_sbp_rr[\n"
    "        (new_sbp_rr['age_start'] == 55) & \n"
    "        (new_sbp_rr['sex'] == 'Male')\n"
    "    ].copy()\n"
    "    if 'affected_entity' in sample.columns:\n"
    "        sample = sample[sample['affected_entity'] == 'acute_ischemic_stroke']\n"
    "    if len(sample) > 0:\n"
    "        sample = sample.sort_values('exposure')\n"
    "        print('SBP RR for Male 55-59 on acute IS (draw_0):')\n"
    "        for _, row in sample.iloc[::max(1, len(sample)//10)].iterrows():\n"
    "            print(f'  exposure={row[\"exposure\"]:6.1f}  RR={row[\"draw_0\"]:.4f}')"
))

# ==========================================
# Section 6: Joint PAF
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 6. Joint PAF Comparison\n"
    "\n"
    "The old artifact stored per-draw PAFs (240 rows x 1000 draws). "
    "The new artifact stores precomputed PAFs (192 rows x 8 targets, "
    "computed from a dedicated PAF-calculation simulation)."
))

cells.append(nbf.v4.new_code_cell(
    "old_paf = old_store['/risk_factor/joint_mediated_risks/population_attributable_fraction'].reset_index()\n"
    "new_paf = new_store['/risk_factor/joint_mediated_risks/population_attributable_fraction'].reset_index()\n"
    "\n"
    "print(f'Old PAF: {old_paf.shape}')\n"
    "print(f'New PAF: {new_paf.shape}')\n"
    "print()\n"
    "print('Old PAF columns:', list(old_paf.columns[:8]))\n"
    "print('New PAF columns:', list(new_paf.columns[:15]))\n"
    "print()\n"
    "\n"
    "# New PAF has target columns; show mean PAFs by target\n"
    "target_cols = [c for c in new_paf.columns if 'incidence_rate' in str(c) or 'transition_rate' in str(c)]\n"
    "if target_cols:\n"
    "    print('New PAF mean by target (ages 25+):')\n"
    "    adult_paf = new_paf[new_paf['age_start'] >= 25]\n"
    "    for col in sorted(target_cols):\n"
    "        print(f'  {col:60s}  mean={adult_paf[col].mean():.3f}')\n"
    "print()\n"
    "\n"
    "# Old PAF: show mean draw_0 by affected_entity\n"
    "if 'affected_entity' in old_paf.columns:\n"
    "    print('Old PAF mean (draw_0) by target (ages 25+):')\n"
    "    old_adult = old_paf[old_paf['age_start'] >= 25]\n"
    "    for entity in sorted(old_adult['affected_entity'].unique()):\n"
    "        subset = old_adult[old_adult['affected_entity'] == entity]\n"
    "        print(f'  {entity:60s}  mean={subset[\"draw_0\"].mean():.3f}')"
))

# ==========================================
# Section 7: Mediation factors
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 7. Mediation Factors\n"
    "\n"
    "Mediation factors should be identical or very close — they represent "
    "the proportion of a risk's effect mediated through each pathway and "
    "are location-independent."
))

cells.append(nbf.v4.new_code_cell(
    "old_mf = old_store['/risk/cause/mediation_factors'].reset_index()\n"
    "new_mf = new_store['/risk/cause/mediation_factors'].reset_index()\n"
    "\n"
    "print(f'Old mediation factors: {old_mf.shape}')\n"
    "print(f'New mediation factors: {new_mf.shape}')\n"
    "print()\n"
    "\n"
    "# Compare values\n"
    "merge_cols = [c for c in old_mf.columns if c != 'value' and c in new_mf.columns]\n"
    "comparison = old_mf.merge(new_mf, on=merge_cols, suffixes=('_old', '_new'))\n"
    "\n"
    "if 'value_old' in comparison.columns and 'value_new' in comparison.columns:\n"
    "    comparison['diff'] = comparison['value_new'] - comparison['value_old']\n"
    "    print(f'Max absolute difference: {comparison[\"diff\"].abs().max():.6f}')\n"
    "    print(f'All identical: {(comparison[\"diff\"].abs() < 1e-10).all()}')\n"
    "    print()\n"
    "    if comparison['diff'].abs().max() > 1e-10:\n"
    "        print('Differences:')\n"
    "        print(comparison[comparison['diff'].abs() > 1e-10].to_string())\n"
    "    else:\n"
    "        print('Mediation factors match exactly (location-independent, as expected).')\n"
    "else:\n"
    "    print('Columns:', list(comparison.columns))"
))

# ==========================================
# Section 8: Medication / treatment data
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 8. Medication and Treatment Data\n"
    "\n"
    "Compare medication adherence exposure and LDL-C medication effect."
))

cells.append(nbf.v4.new_code_cell(
    "# Medication adherence\n"
    "for med in ['sbp_medication_adherence', 'ldlc_medication_adherence']:\n"
    "    old_df = old_store[f'/risk_factor/{med}/exposure'].reset_index()\n"
    "    new_df = new_store[f'/risk_factor/{med}/exposure'].reset_index()\n"
    "    print(f'{med}:')\n"
    "    print(f'  Old: {old_df.shape}')\n"
    "    print(f'  New: {new_df.shape}')\n"
    "    \n"
    "    # Show adherence levels\n"
    "    if 'parameter' in old_df.columns:\n"
    "        for param in sorted(old_df['parameter'].unique()):\n"
    "            old_subset = old_df[old_df['parameter'] == param]\n"
    "            new_subset = new_df[new_df['parameter'] == param] if param in new_df['parameter'].values else pd.DataFrame()\n"
    "            old_val = old_subset['draw_0'].mean()\n"
    "            new_val = new_subset['draw_0'].mean() if len(new_subset) > 0 else float('nan')\n"
    "            print(f'    {param}: old={old_val:.4f}, new={new_val:.4f}')\n"
    "    print()\n"
    "\n"
    "# LDL-C medication effect\n"
    "old_me = old_store['/risk_factor/high_ldl_cholesterol/medication_effect'].reset_index()\n"
    "new_me = new_store['/risk_factor/high_ldl_cholesterol/medication_effect'].reset_index()\n"
    "print('LDL-C medication effect:')\n"
    "print(f'  Old: {old_me.shape}')\n"
    "print(f'  New: {new_me.shape}')\n"
    "print()\n"
    "print('Old columns:', list(old_me.columns[:8]))\n"
    "print('New columns:', list(new_me.columns[:8]))"
))

# ==========================================
# Section 9: Production model run
# ==========================================
cells.append(nbf.v4.new_markdown_cell(
    "## 9. Production Model Run\n"
    "\n"
    "Run the production model specification for 1 year (13 x 28-day steps) "
    "and inspect results."
))

cells.append(nbf.v4.new_code_cell(
    "old_store.close()\n"
    "new_store.close()\n"
    "\n"
    "from vivarium import InteractiveContext\n"
    "\n"
    "yaml_path = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd.yaml'\n"
    "sim = InteractiveContext(yaml_path, setup=False)\n"
    "sim.configuration.update({'population': {'population_size': 2_000}})\n"
    "sim.setup()\n"
    "print('Setup complete.')"
))

cells.append(nbf.v4.new_code_cell(
    "# Run 13 steps (~1 year)\n"
    "for i in range(13):\n"
    "    sim.step()\n"
    "print(f'Simulated 13 steps (364 days).')"
))

# --- Population summary ---
cells.append(nbf.v4.new_code_cell(
    "pop = sim.get_population([\n"
    "    'is_alive', 'age', 'sex',\n"
    "    'ischemic_stroke',\n"
    "    'ischemic_heart_disease_and_heart_failure',\n"
    "])\n"
    "\n"
    "print(f'Population: {len(pop)} simulants')\n"
    "print(f'Alive: {pop[\"is_alive\"].sum()} ({100*pop[\"is_alive\"].mean():.1f}%)')\n"
    "print(f'Deaths: {(~pop[\"is_alive\"]).sum()}')\n"
    "print()\n"
    "\n"
    "for col in ['ischemic_stroke', 'ischemic_heart_disease_and_heart_failure']:\n"
    "    print(f'{col}:')\n"
    "    counts = pop[col].value_counts()\n"
    "    for state, count in counts.items():\n"
    "        print(f'  {state}: {count} ({100*count/len(pop):.1f}%)')\n"
    "    print()"
))

# --- Risk exposures ---
cells.append(nbf.v4.new_code_cell(
    "risk_cols = [\n"
    "    'high_systolic_blood_pressure.exposure',\n"
    "    'high_ldl_cholesterol.exposure',\n"
    "    'high_body_mass_index_in_adults.exposure',\n"
    "    'high_fasting_plasma_glucose.exposure',\n"
    "]\n"
    "\n"
    "risk_pop = sim.get_population(risk_cols + ['age', 'sex', 'is_alive'])\n"
    "alive = risk_pop[risk_pop['is_alive']]\n"
    "\n"
    "print('Risk exposure summary (alive simulants):')\n"
    "print('=' * 70)\n"
    "for col in risk_cols:\n"
    "    vals = alive[col]\n"
    "    short = col.replace('.exposure', '')\n"
    "    print(f'{short:45s}  mean={vals.mean():.2f}  std={vals.std():.2f}  '\n"
    "          f'[{vals.min():.1f}, {vals.max():.1f}]')"
))

# --- Treatment ---
cells.append(nbf.v4.new_code_cell(
    "tx_cols = [\n"
    "    'sbp_medication', 'ldlc_medication',\n"
    "    'sbp_medication_adherence', 'ldlc_medication_adherence',\n"
    "]\n"
    "\n"
    "tx_pop = sim.get_population(tx_cols + ['is_alive'])\n"
    "alive_tx = tx_pop[tx_pop['is_alive']]\n"
    "\n"
    "print('Treatment status (alive simulants):')\n"
    "print('=' * 70)\n"
    "for col in ['sbp_medication', 'ldlc_medication']:\n"
    "    print(f'\\n{col}:')\n"
    "    counts = alive_tx[col].value_counts()\n"
    "    for val, count in counts.items():\n"
    "        print(f'  {val}: {count} ({100*count/len(alive_tx):.1f}%)')\n"
    "print()\n"
    "for col in ['sbp_medication_adherence', 'ldlc_medication_adherence']:\n"
    "    print(f'{col}:')\n"
    "    counts = alive_tx[col].value_counts()\n"
    "    for val, count in counts.items():\n"
    "        print(f'  {val}: {count} ({100*count/len(alive_tx):.1f}%)')\n"
    "    print()"
))

# --- Results ---
cells.append(nbf.v4.new_code_cell(
    "results = sim.get_results()\n"
    "print(f'Total result categories: {len(results)}')\n"
    "print()\n"
    "\n"
    "# Key metrics\n"
    "for key in ['deaths', 'ylls', 'ylds']:\n"
    "    if key in results:\n"
    "        df = results[key]\n"
    "        total = df['value'].sum()\n"
    "        print(f'{key}: {total:.1f}')\n"
    "\n"
    "print()\n"
    "\n"
    "# Disease transitions\n"
    "for key in sorted(results.keys()):\n"
    "    if 'transition_count' in key:\n"
    "        df = results[key]\n"
    "        total = df['value'].sum()\n"
    "        print(f'{key}: {total:.0f} transitions')\n"
    "\n"
    "print()\n"
    "\n"
    "# Healthcare visits\n"
    "for key in sorted(results.keys()):\n"
    "    if 'healthcare_visits' in key:\n"
    "        df = results[key]\n"
    "        total = df['value'].sum()\n"
    "        print(f'{key}: {total:.0f}')"
))

# --- Summary ---
cells.append(nbf.v4.new_markdown_cell(
    "## Summary\n"
    "\n"
    "### Expected differences (GBD round + location change)\n"
    "- Absolute rate levels differ (Alabama vs USA national)\n"
    "- Age bins expanded (23 -> 25 groups)\n"
    "- SBP/LDL-C/FPG RRs switched to non-log-linear format (200K rows)\n"
    "- Joint PAF format changed (per-draw -> precomputed)\n"
    "\n"
    "### Should be preserved\n"
    "- Mediation factors (location-independent)\n"
    "- Exposure distribution weight structure (13 distributions per bin)\n"
    "- General simulation behavior: disease transitions, treatment uptake, mortality\n"
    "\n"
    "### Known limitations\n"
    "- BMI -> IS/MI relative risks are stubs (RR=1.0)\n"
    "- PAFs will need recomputation after real BMI data is added"
))

nb.cells = cells
nbf.write(nb, "notebooks/12_validation_old_vs_new.ipynb")
print("Wrote notebooks/12_validation_old_vs_new.ipynb")
