#!/usr/bin/env python
"""
Build a USA-level artifact by population-weighted aggregation of all 51 state artifacts.

For location-independent data (relative risks, TMREDs, distribution types, etc.),
values are copied directly from any state artifact.

For location-specific data (rates, prevalences, exposures, etc.), values are
aggregated using population-weighted averages across all states.

Usage:
    python build_usa_artifact.py [--artifact-dir ARTIFACT_DIR] [--output OUTPUT]
"""
import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import tables

LOCATIONS = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]

# Keys that are identical across all states -- copy directly from template
LOCATION_INDEPENDENT_KEYS = {
    "/cause/post_myocardial_infarction/disability_weight",
    "/population/age_bins",
    "/population/theoretical_minimum_risk_life_expectancy",
    "/risk/cause/mediation_factors",
    "/risk_factor/categorical_high_systolic_blood_pressure/relative_risk",
    "/risk_factor/high_body_mass_index_in_adults/exposure_distribution_weights",
    "/risk_factor/high_body_mass_index_in_adults/relative_risk",
    "/risk_factor/high_body_mass_index_in_adults_effect_on_heart_failure/mediation_deltas",
    "/risk_factor/high_fasting_plasma_glucose/exposure_distribution_weights",
    "/risk_factor/high_fasting_plasma_glucose/exposure_standard_deviation",
    "/risk_factor/high_fasting_plasma_glucose/relative_risk",
    "/risk_factor/high_ldl_cholesterol/exposure_distribution_weights",
    "/risk_factor/high_ldl_cholesterol/medication_effect",
    "/risk_factor/high_ldl_cholesterol/relative_risk",
    "/risk_factor/high_systolic_blood_pressure/exposure_distribution_weights",
    "/risk_factor/high_systolic_blood_pressure/relative_risk",
}

# Standard demographic keys: pop-weighted average
POP_WEIGHTED_AVERAGE_KEYS = [
    "/cause/acute_myocardial_infarction/disability_weight",
    "/cause/acute_myocardial_infarction/excess_mortality_rate",
    "/cause/acute_myocardial_infarction/incidence_rate",
    "/cause/acute_myocardial_infarction/prevalence",
    "/cause/acute_myocardial_infarction_and_heart_failure/prevalence",
    "/cause/all_causes/cause_specific_mortality_rate",
    "/cause/heart_failure/excess_mortality_rate",
    "/cause/heart_failure_from_ischemic_heart_disease/disability_weight",
    "/cause/heart_failure_from_ischemic_heart_disease/incidence_rate",
    "/cause/heart_failure_from_ischemic_heart_disease/prevalence",
    "/cause/heart_failure_residual/disability_weight",
    "/cause/heart_failure_residual/incidence_rate",
    "/cause/heart_failure_residual/prevalence",
    "/cause/ischemic_heart_disease_and_heart_failure/cause_specific_mortality_rate",
    "/cause/ischemic_stroke/cause_specific_mortality_rate",
    "/cause/ischemic_stroke/incidence_rate",
    "/cause/post_myocardial_infarction/excess_mortality_rate",
    "/cause/post_myocardial_infarction/prevalence",
    "/healthcare_entity/outpatient_visits/outpatient_envelope",
    "/risk_factor/high_body_mass_index_in_adults/exposure",
    "/risk_factor/high_body_mass_index_in_adults/exposure_standard_deviation",
    "/risk_factor/high_fasting_plasma_glucose/exposure",
    "/risk_factor/high_ldl_cholesterol/exposure",
    "/risk_factor/high_ldl_cholesterol/exposure_standard_deviation",
    "/risk_factor/high_systolic_blood_pressure/exposure",
    "/risk_factor/high_systolic_blood_pressure/exposure_standard_deviation",
    "/sequela/acute_ischemic_stroke/disability_weight",
    "/sequela/acute_ischemic_stroke/excess_mortality_rate",
    "/sequela/acute_ischemic_stroke/prevalence",
    "/sequela/chronic_ischemic_stroke/disability_weight",
    "/sequela/chronic_ischemic_stroke/excess_mortality_rate",
    "/sequela/chronic_ischemic_stroke/prevalence",
]


def artifact_path_for(artifact_dir: Path, location: str) -> Path:
    return artifact_dir / (location.lower().replace(" ", "_") + ".hdf")


def _compute_subbin_weights(
    df_reset: pd.DataFrame,
    pop_state: pd.Series,
    pop_total: pd.Series,
    demo_cols: list,
) -> np.ndarray:
    """Compute per-row state weights robust to age-bin mismatches.

    For each demographic row in ``df_reset``, returns
    ``state_pop(row_bin) / total_pop(row_bin)``, where ``row_bin`` may be
    wider than a single 5-year sub-bin in the population tables (e.g. the
    joint PAF table's (5, 25) bin). The implementation sums the
    population series across all 5-year sub-bins whose ``[age_start, age_end)``
    falls within the target row's range and matches on the remaining
    demographic columns. When the target bin matches a 5-year sub-bin
    exactly, the result is identical to ``pop_state / pop_total``.
    """
    ps = pop_state.reset_index()
    ps.columns = list(ps.columns[:-1]) + ["_state_pop"]
    pt = pop_total.reset_index()
    pt.columns = list(pt.columns[:-1]) + ["_total_pop"]

    target_bins = df_reset[demo_cols].drop_duplicates().reset_index(drop=True)

    state_sums = np.zeros(len(target_bins), dtype=np.float64)
    total_sums = np.zeros(len(target_bins), dtype=np.float64)
    for i, row in target_bins.iterrows():
        mask_s = np.ones(len(ps), dtype=bool)
        mask_t = np.ones(len(pt), dtype=bool)
        for col in demo_cols:
            if col == "age_start":
                mask_s &= (ps["age_start"] >= row["age_start"]).values
                mask_t &= (pt["age_start"] >= row["age_start"]).values
            elif col == "age_end":
                mask_s &= (ps["age_end"] <= row["age_end"]).values
                mask_t &= (pt["age_end"] <= row["age_end"]).values
            else:
                mask_s &= (ps[col] == row[col]).values
                mask_t &= (pt[col] == row[col]).values
        state_sums[i] = ps.loc[mask_s, "_state_pop"].sum()
        total_sums[i] = pt.loc[mask_t, "_total_pop"].sum()

    with np.errstate(divide="ignore", invalid="ignore"):
        bin_weights = np.where(total_sums > 0, state_sums / total_sums, 0.0)
    target_bins["_weight"] = bin_weights

    merged = df_reset.merge(target_bins, on=demo_cols, how="left")
    return merged["_weight"].values.astype(np.float64)


def write_df(output_path: str, key: str, df: pd.DataFrame) -> None:
    """Write a DataFrame to HDF5 matching vivarium's artifact convention.

    Uses vivarium's own _write_pandas_data to ensure correct metadata and
    format. DataFrames without draw/value columns need to have their
    non-numeric columns set as index to match the "is_empty" convention.
    """
    from vivarium.framework.artifact.hdf import EntityKey, _write_pandas_data

    # vivarium's convention: DataFrames that don't contain draw_*/value columns
    # are written with all columns set as index, making data.empty=True, which
    # triggers is_empty=True metadata and data_columns=True storage.
    write_df = df.copy()
    draw_or_value_cols = [
        c for c in write_df.columns if c.startswith("draw_") or c == "value"
    ]
    if not draw_or_value_cols:
        # No data columns — set all columns as index (matching vivarium convention)
        all_cols = list(write_df.columns)
        if write_df.index.names == [None]:
            write_df = write_df.set_index(all_cols)

    entity_key_str = key.strip("/").replace("/", ".")
    _write_pandas_data(Path(output_path), EntityKey(entity_key_str), write_df)


def write_json_node(h5file, group_path: str, name: str, data) -> None:
    """Write a JSON blob as a filenode (matching vivarium artifact convention)."""
    from tables.nodes import filenode

    # Ensure parent group exists
    parts = group_path.strip("/").split("/")
    current = "/"
    for part in parts:
        new_path = current.rstrip("/") + "/" + part
        if new_path not in h5file:
            h5file.create_group(current, part)
        current = new_path
    with filenode.new_node(h5file, where=group_path, name=name) as fnode:
        fnode.write(bytes(json.dumps(data), "utf-8"))


def copy_json_node(src_h5, dst_h5, path: str) -> None:
    """Copy a filenode (JSON blob) from source to destination HDF5 file."""
    from tables.nodes import filenode

    node = src_h5.get_node(path)
    raw = bytes(node.read())
    data = json.loads(raw)
    parent = "/".join(path.split("/")[:-1]) or "/"
    name = path.split("/")[-1]
    write_json_node(dst_h5, parent, name, data)


def build_usa_artifact(artifact_dir: Path, output_path: Path) -> None:
    """Build the USA-level artifact."""

    # Step 1: Load all population structures
    print("Loading population structures...")
    pop_by_state = {}
    available_states = []
    for loc in LOCATIONS:
        path = artifact_path_for(artifact_dir, loc)
        if not path.exists():
            print(f"  WARNING: Missing artifact for {loc}")
            continue
        pop = pd.read_hdf(path, "/population/structure")
        pop = pop.reset_index()
        if "location" in pop.columns:
            pop = pop.drop(columns=["location"])
        pop = pop.set_index(["sex", "age_start", "age_end", "year_start", "year_end"])
        pop_by_state[loc] = pop["value"]
        available_states.append(loc)

    print(f"  Loaded {len(available_states)} states")

    # Compute weights per demographic cell
    total_pop = sum(pop_by_state.values())
    weights = {loc: pop_by_state[loc] / total_pop for loc in available_states}

    template_path = artifact_path_for(artifact_dir, available_states[0])

    # Step 2: Build new artifact from scratch
    if output_path.exists():
        os.remove(output_path)

    output_str = str(output_path)

    # Step 3: Copy non-DataFrame (Array) nodes from template
    print("Copying metadata and array nodes from template...")
    src_h5 = tables.open_file(str(template_path), "r")
    dst_h5 = tables.open_file(output_str, "w")

    # Copy all Array nodes (metadata, distributions, restrictions, location)
    for node in src_h5.walk_nodes("/", classname="Array"):
        path = node._v_pathname
        # Skip EArray (keyspace) - we'll regenerate
        if path == "/metadata/keyspace":
            continue
        copy_json_node(src_h5, dst_h5, path)

    # Update location-specific array nodes
    # Remove old location and locations, write new ones
    try:
        dst_h5.remove_node("/population/location")
    except tables.NoSuchNodeError:
        pass
    try:
        dst_h5.remove_node("/metadata/locations")
    except tables.NoSuchNodeError:
        pass

    write_json_node(dst_h5, "/population", "location", "United States of America")
    write_json_node(dst_h5, "/metadata", "locations", ["United States of America"])

    src_h5.close()
    dst_h5.close()

    # Step 4: Copy location-independent DataFrame keys from template
    print("Copying location-independent data...")
    for key in sorted(LOCATION_INDEPENDENT_KEYS):
        print(f"  {key}", flush=True)
        df = pd.read_hdf(str(template_path), key)
        write_df(output_str, key, df)

    # Step 5: Compute pop-weighted averages for standard demographic keys
    print("Computing population-weighted averages...")
    for key in POP_WEIGHTED_AVERAGE_KEYS:
        print(f"  {key}", flush=True)

        template_df = pd.read_hdf(str(template_path), key)
        draw_cols = [c for c in template_df.columns if c.startswith("draw_")]
        val_cols = draw_cols if draw_cols else ["value"]

        result_values = np.zeros_like(template_df[val_cols].values, dtype=np.float64)

        for loc in available_states:
            path = artifact_path_for(artifact_dir, loc)
            df = pd.read_hdf(str(path), key)
            data = df[val_cols].values.astype(np.float64)

            w = weights[loc]
            df_reset = df.reset_index()
            demo_cols = [
                c
                for c in ["sex", "age_start", "age_end", "year_start", "year_end"]
                if c in df_reset.columns
            ]
            if demo_cols:
                w_reset = w.reset_index()
                w_reset.columns = list(w_reset.columns[:-1]) + ["_weight"]
                merged = df_reset.merge(w_reset, on=demo_cols, how="left")
                weight_arr = merged["_weight"].values.astype(np.float64)
            else:
                weight_arr = np.full(len(df), 1.0 / len(available_states))

            result_values += data * weight_arr[:, np.newaxis]

        result_df = template_df.copy()
        result_df[val_cols] = result_values
        write_df(output_str, key, result_df)

    # Step 6: Joint PAFs
    #
    # The joint PAF table has an irregular (5, 25) age bin (the PAF calculation
    # simulation aggregates ages 5-25 into one bin for affected entities), while
    # the population.structure tables use 5-year bins. A direct merge on
    # age_start/age_end would produce NaN weights for the (5, 25) rows, which
    # propagates through rate * (1 - PAF) and breaks the simulation. We fix this
    # by computing the weight as
    #     state_pop(target_bin) / total_pop(target_bin)
    # where state_pop and total_pop are summed over all 5-year sub-bins falling
    # within [age_start, age_end). This reduces to the direct lookup when the
    # target bin matches a 5-year sub-bin exactly.
    print("  /risk_factor/joint_mediated_risks/population_attributable_fraction", flush=True)
    key = "/risk_factor/joint_mediated_risks/population_attributable_fraction"
    template_df = pd.read_hdf(str(template_path), key)
    draw_cols = [c for c in template_df.columns if c.startswith("draw_")]
    result_values = np.zeros_like(template_df[draw_cols].values, dtype=np.float64)

    demo_cols = ["sex", "age_start", "age_end", "year_start", "year_end"]

    for loc in available_states:
        path = artifact_path_for(artifact_dir, loc)
        df = pd.read_hdf(str(path), key)
        data = df[draw_cols].values.astype(np.float64)
        df_reset = df.reset_index()
        weight_arr = _compute_subbin_weights(
            df_reset, pop_by_state[loc], total_pop, demo_cols
        )
        result_values += data * weight_arr[:, np.newaxis]

    result_df = template_df.copy()
    result_df[draw_cols] = result_values
    write_df(output_str, key, result_df)

    # Step 7: Population structure — SUM
    print("  /population/structure (summing)", flush=True)
    total_pop_df = None
    for loc in available_states:
        path = artifact_path_for(artifact_dir, loc)
        df = pd.read_hdf(str(path), "/population/structure")
        df = df.reset_index()
        if "location" in df.columns:
            df = df.drop(columns=["location"])
        if total_pop_df is None:
            total_pop_df = df.copy()
        else:
            total_pop_df["value"] += df["value"].values

    total_pop_df.insert(0, "location", "United States of America")
    total_pop_df = total_pop_df.set_index(
        ["location", "sex", "age_start", "age_end", "year_start", "year_end"]
    )
    write_df(output_str, "/population/structure", total_pop_df)

    # Step 8: Demographic dimensions — update location
    print("  /population/demographic_dimensions", flush=True)
    demo = pd.read_hdf(str(template_path), "/population/demographic_dimensions")
    demo["location"] = "United States of America"
    write_df(output_str, "/population/demographic_dimensions", demo)

    # Step 9: Medication adherence (has location in index)
    for adherence_key in [
        "/risk_factor/ldlc_medication_adherence/exposure",
        "/risk_factor/sbp_medication_adherence/exposure",
    ]:
        print(f"  {adherence_key}", flush=True)
        template_df = pd.read_hdf(str(template_path), adherence_key)
        draw_cols = [c for c in template_df.columns if c.startswith("draw_")]
        result_values = np.zeros((len(template_df), len(draw_cols)), dtype=np.float64)

        for loc in available_states:
            path = artifact_path_for(artifact_dir, loc)
            df = pd.read_hdf(str(path), adherence_key)
            data = df[draw_cols].values.astype(np.float64)
            df_reset = df.reset_index()
            w = weights[loc]
            w_reset = w.reset_index()
            w_reset.columns = list(w_reset.columns[:-1]) + ["_weight"]
            merged = df_reset.merge(
                w_reset,
                on=["sex", "age_start", "age_end", "year_start", "year_end"],
                how="left",
            )
            weight_arr = merged["_weight"].values.astype(np.float64)
            result_values += data * weight_arr[:, np.newaxis]

        result_df = template_df.copy()
        result_df[draw_cols] = result_values
        result_df = result_df.reset_index()
        result_df["location"] = "United States of America"
        result_df = result_df.set_index(list(template_df.index.names))
        write_df(output_str, adherence_key, result_df)

    # Step 10: Medication coverage scaling factor
    print("  /risk_factor/medication_coverage/scaling_factor", flush=True)
    key = "/risk_factor/medication_coverage/scaling_factor"
    template_df = pd.read_hdf(str(template_path), key)
    value_cols = ["sbp_rr", "ldl_rr", "both_rr"]
    result_values = np.zeros((len(template_df), len(value_cols)), dtype=np.float64)

    for loc in available_states:
        path = artifact_path_for(artifact_dir, loc)
        df = pd.read_hdf(str(path), key)
        data = df[value_cols].values.astype(np.float64)

        state_pop = pop_by_state[loc]
        total_pop_by_demo = state_pop.groupby(["sex", "age_start", "age_end"]).sum()
        total_all_by_demo = total_pop.groupby(["sex", "age_start", "age_end"]).sum()
        w = total_pop_by_demo / total_all_by_demo

        w_reset = w.reset_index()
        w_reset.columns = list(w_reset.columns[:-1]) + ["_weight"]
        merged = df.merge(w_reset, on=["sex", "age_start", "age_end"], how="left")
        weight_arr = merged["_weight"].values.astype(np.float64)
        result_values += data * weight_arr[:, np.newaxis]

    result_df = template_df.copy()
    result_df[value_cols] = result_values
    write_df(output_str, key, result_df)

    # Step 11: Write keyspace metadata
    print("Writing keyspace...", flush=True)
    # Collect all DataFrame keys we wrote
    all_keys = sorted(
        list(LOCATION_INDEPENDENT_KEYS)
        + POP_WEIGHTED_AVERAGE_KEYS
        + [
            "/risk_factor/joint_mediated_risks/population_attributable_fraction",
            "/population/structure",
            "/population/demographic_dimensions",
            "/risk_factor/ldlc_medication_adherence/exposure",
            "/risk_factor/sbp_medication_adherence/exposure",
            "/risk_factor/medication_coverage/scaling_factor",
        ]
    )
    # Also include the Array-based keys
    array_keys = [
        "metadata.keyspace",
        "metadata.locations",
        "population.location",
        "risk_factor.categorical_high_systolic_blood_pressure.distribution",
        "risk_factor.high_body_mass_index_in_adults.distribution",
        "risk_factor.high_body_mass_index_in_adults.relative_risk_scalar",
        "risk_factor.high_body_mass_index_in_adults.tmred",
        "risk_factor.high_fasting_plasma_glucose.distribution",
        "risk_factor.high_fasting_plasma_glucose.relative_risk_scalar",
        "risk_factor.high_fasting_plasma_glucose.tmred",
        "risk_factor.high_ldl_cholesterol.distribution",
        "risk_factor.high_ldl_cholesterol.relative_risk_scalar",
        "risk_factor.high_ldl_cholesterol.tmred",
        "risk_factor.high_systolic_blood_pressure.distribution",
        "risk_factor.high_systolic_blood_pressure.relative_risk_scalar",
        "risk_factor.high_systolic_blood_pressure.tmred",
        "risk_factor.ldlc_medication_adherence.distribution",
        "risk_factor.outreach.distribution",
        "risk_factor.polypill.distribution",
        "risk_factor.sbp_medication_adherence.distribution",
        "cause.ischemic_stroke.restrictions",
        "cause.ischemic_heart_disease_and_heart_failure.restrictions",
    ]
    # Convert HDF paths to dot-separated keys
    df_keys = [k.strip("/").replace("/", ".") for k in all_keys]
    all_keyspace = sorted(set(df_keys + array_keys))
    with tables.open_file(output_str, "a") as f:
        # Remove old keyspace if exists
        try:
            f.remove_node("/metadata/keyspace")
        except tables.NoSuchNodeError:
            pass
        write_json_node(f, "/metadata", "keyspace", all_keyspace)

    us_total = total_pop_df["value"].sum()
    print(f"\nDone! USA artifact: {output_path}")
    print(f"  Total US population: {us_total:,.0f}")
    print(f"  File size: {output_path.stat().st_size / 1e6:.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="Build USA-level artifact")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(__file__).parent / "src/vivarium_nih_us_cvd/artifacts",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent
        / "src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf",
    )
    args = parser.parse_args()

    if not args.artifact_dir.exists():
        print(f"ERROR: Artifact directory not found: {args.artifact_dir}")
        return 1

    build_usa_artifact(args.artifact_dir, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
