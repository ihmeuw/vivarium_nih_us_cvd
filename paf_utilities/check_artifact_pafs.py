import argparse
from pathlib import Path

from vivarium import Artifact

root_dir = Path(
    "/mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/v1-20230613"
)
expected_num_paf_keys = 5


def main(root_dir: Path) -> None:
    bad_files = []
    for file in sorted(list(root_dir.glob("*.hdf"))):
        filename = file.name
        print(f"Checking {filename}")
        art = Artifact(file)
        num_paf_keys = len([k for k in art.keys if "population_attributable_fraction" in k])
        if num_paf_keys != expected_num_paf_keys:
            print(f"\nMISSING! - {filename}: {num_paf_keys} PAF keys\n")
            bad_files.append(filename)

    if bad_files:
        print("\n*** FINISHED ***")
        print(
            f"Some artifacts were found without the required {expected_num_paf_keys} PAF keys:"
        )
        print(f"{bad_files}")
    else:
        print("\n*** FINISHED ***")
        print(f"All artifacts have the expected number of PAF keys ({expected_num_paf_keys})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--directory",
        default="/mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/",
        help="Root artifacts directory",
    )
    parser.add_argument(
        "-v",
        "--version",
        help="Version/sub-folder of artifacts to check",
    )
    directory = parser.parse_args().directory
    version = parser.parse_args().version
    root_dir = Path(directory) / version
    if not root_dir.exists():
        raise RuntimeError(f"Directory does not exist: {root_dir}")

    main(root_dir)