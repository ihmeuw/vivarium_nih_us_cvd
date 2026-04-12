"""Build notebook 11: Phase 7 — Mediated Risk Effects + Joint PAF demo."""
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
    "# Phase 7: Mediated Risk Effects + Joint PAF\n"
    "\n"
    "This notebook demonstrates the Phase 7 model build-up, which adds:\n"
    "\n"
    "- **MediatedRiskEffect** (log-linear): BMI on IS/MI and HF targets, "
    "categorical SBP on HF\n"
    "- **NonLogLinearMediatedRiskEffect**: SBP, LDL-C, FPG on IS/MI targets\n"
    "- **JointPAF**: population-attributable fractions computed from a "
    "dedicated PAF-calculation simulation and cached in the artifact\n"
    "\n"
    "The mediation math adjusts each risk's relative risk to remove the "
    "portion of its effect that operates through downstream mediators "
    "(e.g., BMI's effect on MI is partially mediated by SBP, LDL-C, and FPG).\n"
    "\n"
    "> **Note:** BMI relative risk data for IS/MI targets is currently **stub data** "
    "(RR=1.0, i.e. no direct effect). The GBD 2023 artifact was missing these "
    "entries. This needs investigation — see `memory/project_bmi_is_mi_investigation.md`."
))

# --- Setup ---
cells.append(nbf.v4.new_code_cell(
    "from vivarium import InteractiveContext\n"
    "import pandas as pd\n"
    "import numpy as np\n"
    "\n"
    "yaml_path = '../src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd_phase7.yaml'\n"
    "sim = InteractiveContext(yaml_path, setup=False)\n"
    "sim.configuration.update({'population': {'population_size': 1_000}})\n"
    "sim.setup()\n"
    "print('Setup complete.')"
))

# --- Component inventory ---
cells.append(nbf.v4.new_markdown_cell(
    "## Component inventory\n"
    "\n"
    "Phase 7 adds 20 mediated risk effects (vs. 3 simple effects in Phase 6) "
    "plus the JointPAF component."
))

cells.append(nbf.v4.new_code_cell(
    "components = sim._component_manager.list_components()\n"
    "\n"
    "log_linear = [c for c in components if c.startswith('risk_effect.')]\n"
    "non_ll = [c for c in components if c.startswith('non_log_linear_risk_effect.')]\n"
    "joint_paf = [c for c in components if 'joint_paf' in c]\n"
    "\n"
    "print(f'Log-linear mediated effects: {len(log_linear)}')\n"
    "print(f'Non-log-linear mediated effects: {len(non_ll)}')\n"
    "print(f'JointPAF components: {len(joint_paf)}')\n"
    "print()\n"
    "print('All risk effect components:')\n"
    "for c in sorted(log_linear + non_ll):\n"
    "    print(f'  {c}')"
))

# --- Run simulation ---
cells.append(nbf.v4.new_markdown_cell(
    "## Run a short simulation\n"
    "\n"
    "Step for ~6 months (6 x 28-day steps) and inspect disease transitions."
))

cells.append(nbf.v4.new_code_cell(
    "for i in range(6):\n"
    "    sim.step()\n"
    "print(f'Simulated through 6 steps (168 days).')"
))

cells.append(nbf.v4.new_code_cell(
    "pop = sim.get_population([\n"
    "    'is_alive', 'age', 'sex',\n"
    "    'ischemic_stroke',\n"
    "    'ischemic_heart_disease_and_heart_failure',\n"
    "])\n"
    "\n"
    "print(f'Population: {len(pop)} simulants, {pop[\"is_alive\"].sum()} alive')\n"
    "print()\n"
    "for col in ['ischemic_stroke', 'ischemic_heart_disease_and_heart_failure']:\n"
    "    print(f'{col}:')\n"
    "    print(pop[col].value_counts().to_string())\n"
    "    print()"
))

# --- PAF inspection ---
cells.append(nbf.v4.new_markdown_cell(
    "## Population-Attributable Fractions\n"
    "\n"
    "The JointPAF component loads precomputed PAFs from the artifact and applies "
    "them as modifiers on each target rate's `.paf` pipeline. "
    "The PAF = (E[RR] - 1) / E[RR] is computed per age/sex stratum in a "
    "dedicated PAF-calculation simulation with 100K simulants."
))

cells.append(nbf.v4.new_code_cell(
    "paf_cols = [\n"
    "    'acute_ischemic_stroke.incidence_rate.paf',\n"
    "    'acute_myocardial_infarction.incidence_rate.paf',\n"
    "    'heart_failure_from_ischemic_heart_disease.incidence_rate.paf',\n"
    "    'heart_failure_residual.incidence_rate.paf',\n"
    "]\n"
    "\n"
    "paf_pop = sim.get_population(paf_cols + ['age', 'sex'])\n"
    "\n"
    "print('PAF summary by target rate:')\n"
    "print('=' * 70)\n"
    "for col in paf_cols:\n"
    "    vals = paf_pop[col]\n"
    "    short_name = col.replace('.incidence_rate.paf', '')\n"
    "    print(f'{short_name:50s}  mean={vals.mean():.3f}  [{vals.min():.3f}, {vals.max():.3f}]')\n"
    "print()\n"
    "print('PAF of 0 in the youngest bin is expected (risks have no effect below age 25).')"
))

# --- PAF by age and sex ---
cells.append(nbf.v4.new_code_cell(
    "# PAF by age group for acute IS\n"
    "paf_pop['age_group'] = pd.cut(\n"
    "    paf_pop['age'],\n"
    "    bins=[0, 25, 35, 45, 55, 65, 75, 85, 130],\n"
    "    labels=['<25', '25-34', '35-44', '45-54', '55-64', '65-74', '75-84', '85+'],\n"
    "    right=False,\n"
    ")\n"
    "\n"
    "paf_by_age = paf_pop.groupby(['age_group', 'sex'])[\n"
    "    'acute_ischemic_stroke.incidence_rate.paf'\n"
    "].mean().unstack('sex')\n"
    "\n"
    "print('IS incidence PAF by age and sex:')\n"
    "print(paf_by_age.round(3).to_string())"
))

# --- Mediation math ---
cells.append(nbf.v4.new_markdown_cell(
    "## How mediation works\n"
    "\n"
    "For a risk like BMI affecting MI, the effect is mediated by SBP, LDL-C, "
    "and FPG. The mediated target modifier computes:\n"
    "\n"
    "```\n"
    "adjusted_rate = base_rate * unadjusted_RR / scaling_factor\n"
    "```\n"
    "\n"
    "where `scaling_factor` accounts for the portion of BMI's effect operating "
    "through each mediator:\n"
    "\n"
    "```\n"
    "for each mediator:\n"
    "    mf = mediation_factor(risk, mediator, target)\n"
    "    delta = log(mf * (RR_risk - 1) + 1) / log(RR_mediator)\n"
    "    scaling_factor *= RR_mediator^delta\n"
    "```\n"
    "\n"
    "For heart failure targets, precomputed delta values from the artifact "
    "are used instead of the mediation factor formula.\n"
    "\n"
    "The `MEDIATOR_NAMES` dict defines which risks mediate which targets:"
))

cells.append(nbf.v4.new_code_cell(
    "from vivarium_nih_us_cvd.components.effects import MEDIATOR_NAMES\n"
    "\n"
    "for risk, targets in MEDIATOR_NAMES.items():\n"
    "    print(f'{risk}:')\n"
    "    for target, mediators in targets.items():\n"
    "        print(f'  {target} -> mediators: {mediators}')\n"
    "    print()"
))

# --- Observer results ---
cells.append(nbf.v4.new_markdown_cell(
    "## Observer results\n"
    "\n"
    "Phase 7 inherits all observers from Phase 6: mortality, disability, "
    "disease, healthcare visits, medication, lifestyle."
))

cells.append(nbf.v4.new_code_cell(
    "results = sim.get_results()\n"
    "print(f'Total result measures: {len(results)}')\n"
    "print()\n"
    "for key in sorted(results.keys()):\n"
    "    df = results[key]\n"
    "    print(f'  {key}: {df.shape}')"
))

# --- Stub warning ---
cells.append(nbf.v4.new_markdown_cell(
    "## Known limitations\n"
    "\n"
    "- **BMI -> IS/MI relative risks are stubs (RR=1.0)**. The GBD 2023 "
    "artifact did not include BMI relative risk data for ischemic stroke or "
    "myocardial infarction targets. Stub data has been added so the model "
    "structure is complete, but these need to be replaced with real data "
    "before production runs. The model structure (mediation by SBP, LDL-C, "
    "FPG) matches the GBD 2020 specification.\n"
    "\n"
    "- **PAFs need to be recomputed** after the BMI->IS/MI RR data is updated."
))

nb.cells = cells
nbf.write(nb, "notebooks/11_phase7_mediated_effects.ipynb")
print("Wrote notebooks/11_phase7_mediated_effects.ipynb")
