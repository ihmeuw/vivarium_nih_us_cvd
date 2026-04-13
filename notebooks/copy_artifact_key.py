#!/usr/bin/env python
"""Copy a single HDF key from one artifact into another.

Usage
-----
    python copy_artifact_key.py SOURCE_ARTIFACT DEST_ARTIFACT KEY [KEY ...]

Examples
--------
    # Copy California AMI incidence into the USA artifact
    python copy_artifact_key.py \\
        ../src/vivarium_nih_us_cvd/artifacts/california.hdf \\
        ../src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf \\
        /cause/acute_myocardial_infarction/incidence_rate

    # Copy multiple keys at once
    python copy_artifact_key.py source.hdf dest.hdf \\
        /cause/acute_myocardial_infarction/incidence_rate \\
        /cause/acute_myocardial_infarction/excess_mortality_rate

Notes
-----
- The destination key is overwritten if it already exists.
- Both artifacts must be HDF5 files readable by pandas.
- Preserves the pandas storer ``metadata`` attribute (required by
  vivarium's ``hdf.load``).
- The source data is written with the same index structure it had
  in the source artifact (no reindexing to match destination demographics).
"""
import argparse
import sys

import pandas as pd


def copy_key(source_path: str, dest_path: str, key: str) -> None:
    """Read *key* from *source_path* and write it into *dest_path*,
    preserving the pandas storer metadata attribute."""
    # Read the data and its metadata from the source
    df = pd.read_hdf(source_path, key)
    with pd.HDFStore(source_path, mode="r") as src:
        storer = src.get_storer(key)
        metadata = getattr(storer.attrs, "metadata", {"is_empty": False})

    # Write the data into the destination
    with pd.HDFStore(dest_path, mode="a") as dest:
        if key in dest:
            dest.remove(key)
        dest.put(key, df, format="table")
        # Restore the metadata attribute that vivarium's hdf.load expects
        dest.get_storer(key).attrs.metadata = metadata

    print(f"  {key}  ({df.shape[0]} rows x {df.shape[1]} cols)")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Copy HDF key(s) from one artifact to another."
    )
    parser.add_argument("source", help="Source artifact (.hdf)")
    parser.add_argument("dest", help="Destination artifact (.hdf)")
    parser.add_argument("keys", nargs="+", help="HDF key(s) to copy")
    args = parser.parse_args(argv)

    print(f"Copying from {args.source} -> {args.dest}")
    for key in args.keys:
        copy_key(args.source, args.dest, key)
    print("Done.")


if __name__ == "__main__":
    main()
