#!/usr/bin/env python
"""
Run the vivarium NIH US CVD simulation for all 51 US locations.

Usage:
    python run_all_states.py [--output-dir OUTPUT_DIR] [--pop-size POP_SIZE] [--end-year END_YEAR]

By default runs with 10,000 simulants per state, from 2021 to 2040.
"""
import argparse
import sys
import time
from pathlib import Path

from vivarium import InteractiveContext


LOCATIONS = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
]

MODEL_SPEC = Path(__file__).parent / "src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd.yaml"
ARTIFACT_DIR = Path(__file__).parent / "src/vivarium_nih_us_cvd/artifacts"


def run_state(location: str, output_dir: Path, pop_size: int, end_year: int) -> None:
    artifact_name = location.lower().replace(" ", "_") + ".hdf"
    artifact_path = ARTIFACT_DIR / artifact_name

    if not artifact_path.exists():
        print(f"  SKIP: Artifact not found: {artifact_path}")
        return

    state_output = output_dir / location.lower().replace(" ", "_")
    state_output.mkdir(parents=True, exist_ok=True)

    print(f"  Running {location} (pop={pop_size}, end_year={end_year})...")
    t0 = time.time()

    sim = InteractiveContext(
        str(MODEL_SPEC),
        configuration={
            "input_data": {"artifact_path": str(artifact_path)},
            "population": {"population_size": pop_size},
            "time": {"end": {"year": end_year, "month": 12, "day": 31}},
        },
    )
    sim.run()

    # Save results
    pop = sim.get_population()
    pop.to_hdf(str(state_output / "final_population.hdf"), key="population")

    elapsed = time.time() - t0
    n_alive = len(pop[pop["alive"] == "alive"])
    print(f"  Done: {location} in {elapsed:.1f}s ({n_alive}/{len(pop)} alive)")


def main():
    parser = argparse.ArgumentParser(description="Run CVD sim for all US states")
    parser.add_argument("--output-dir", type=Path, default=Path("sim_output"))
    parser.add_argument("--pop-size", type=int, default=10_000)
    parser.add_argument("--end-year", type=int, default=2040)
    parser.add_argument("--states", nargs="*", help="Run only specific states")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    states = args.states if args.states else LOCATIONS
    print(f"Running simulation for {len(states)} locations")
    print(f"Output: {args.output_dir.absolute()}")
    print(f"Pop size: {args.pop_size}, End year: {args.end_year}")
    print()

    failed = []
    for i, state in enumerate(states, 1):
        print(f"[{i}/{len(states)}] {state}")
        try:
            run_state(state, args.output_dir, args.pop_size, args.end_year)
        except Exception as e:
            print(f"  FAILED: {e}")
            failed.append(state)

    print(f"\nCompleted: {len(states) - len(failed)}/{len(states)}")
    if failed:
        print(f"Failed: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
