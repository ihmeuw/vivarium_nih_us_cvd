Artifact generation is currently a multi-step process as outlined in this README.


NOTE: This is the workflow implemented as of 2023-06-13.
NOTE: The following notes assume that the official artifact directory is located at /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/.
TODO: This workflow needs to be updated as it is refined (feature branches are merged, etc).


STEP 1: SET UP ENV
------------------
1. Set up the environment
    ```
    >>> conda create -n <ENV-NAME> python=3.9
    >>> conda activate <ENV-NAME>
    >>> cd <REPOS-DIR>/vivarium_nih_us_cvd; pip install -e .
    >>> cd <REPOS-DIR>/vivarium_public_health; pip install -e .; git checkout feature/pafs_for_all_states
    >>> cd <REPOS-DIR>/vivarium_nih_us_cvd; git checkout feature/pafs_for_all_states
    ```


STEP 2: GENERATE ARTIFACTS (WITHOUT PAFS)
-----------------------------------------
1. Comment out the five PAF items in ../src/vivarium_nih_us_cvd/constants/data_keys.py:
    - __HighLDLCholesterol().PAF
    - __HighSBP().PAF
    - __HighSBP().CATEGORICAL_PAF
    - __HighBMI().PAF
    - __HighFPG().PAF
2. Comment out the five PAFs in the mapping dictionary in ../src/vivarium_nih_us_cvd/data/loader.py:
    - data_keys.LDL_C.PAF: partial(load_paf_ldl, artifact_path),
    - data_keys.SBP.PAF: partial(load_paf_sbp, artifact_path),
    - data_keys.SBP.CATEGORICAL_PAF: partial(load_paf_categorical_sbp, artifact_path),
    - data_keys.BMI.PAF: partial(load_paf_bmi, artifact_path),
    - data_keys.FPG.PAF: load_standard_data,
3. Comment out the `is_calculated_paf` logic in ..src.vivarium_nih_us_cvd.data.loader::match_rr_to_cause_name(). Note that the value of `is_calculated_paf` should be False.
4. Create a new <VERSION> folder (e.g. v1-20230613) for the artifacts 
    ```
    >>> mkdir /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>
    ```
5. Request a cluster node appropriate to make artifacts for all locations. This may take some trial and error! For reference, it took ~3.5 hours to generate the PAF-less artifact for Alabama the first time (and then MUCH less afterwards due to gbd call caching).
6. Run make_artifacts on all locations
    ```
    >>> make_artifacts -vvv --pdb -l all -o /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>
    ```


STEP 3: CALCULATE PAFS
----------------------
1. Request cluster resources appropriate for running the PAF simulations. This may take some trial and error! A good starting point is 3 threads, 11 hours, 50 GB
2. Make a `paf-calculations` subdirectory in the current artifact version folder (alongside the artifacts already generated)
    ```
    >>> mkdir -p /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>/paf-calculations
    ```
3. (OPTIONAL) Update the cluster requests in ./paf_runner.py
4. Run the ./calculate_pafs.sh script to launch a `psimulate run` command for all locations (defined in the shell script)
    ```
    >>> sh ~/repos/vivarium_nih_us_cvd/paf_utilities/calculate_pafs.sh run <VERSION> <ROOT-DIR>
    ```
    NOTE: the <ROOT-DIR> is optional and has a sensible default.
5. Review the output from ./calculate_pafs.sh and note any states that do not have 1000 rows generated. These are the states that require a `psimulate restart`.
6. Comment out all states in the locations list in ./calculate_pafs.sh except those requiring a restart. Note that this is not strictly required as `psimulate restart` will check for complete sims and just not do anything in that case, but not requesting resources for these unnecessary runs will help with our slurm fairshare.
7. Run the ./calculate_pafs.sh script to launch a `psimulate restart` command for all locations requiring a restart
    ```
    >>> sh ~/repos/vivarium_nih_us_cvd/paf_utilities/calculate_pafs.sh restart <VERSION> <ROOT-DIR>
    ```
8. Repeat the previous two steps until all locations have complete PAF outputs (1000 draws).

NOTE: ./calculate_pafs.sh calls ./check_completion.sh as the final step; you can always run ./check_completion.sh on its own.


STEP 4: ADD PAFS TO THE ARTIFACTS
---------------------------------
1. Change branches
    >>> cd ../vivarium_public_health; git checkout main
2. Uncomment the five PAF items in ../src/vivarium_nih_us_cvd/constants/data_keys.py:
    - __HighLDLCholesterol().PAF
    - __HighSBP().PAF
    - __HighSBP().CATEGORICAL_PAF
    - __HighBMI().PAF
    - __HighFPG().PAF
3. Uncomment the five PAFs in the mapping dictionary in ../src/vivarium_nih_us_cvd/data/loader.py:
    - data_keys.LDL_C.PAF: partial(load_paf_ldl, artifact_path),
    - data_keys.SBP.PAF: partial(load_paf_sbp, artifact_path),
    - data_keys.SBP.CATEGORICAL_PAF: partial(load_paf_categorical_sbp, artifact_path),
    - data_keys.BMI.PAF: partial(load_paf_bmi, artifact_path),
    - data_keys.FPG.PAF: load_standard_data,
4. Uncomment the `is_calculated_paf` logic in ..src.vivarium_nih_us_cvd.data.loader::match_rr_to_cause_name(). Note that the value of `is_calculated_paf` should now not necessarily be False.
5. Append the new PAF data to the artifacts
    ```
    >>> make_artifacts -vvv --pdb -l all -o /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION> -a
    ```
6. Confirm that all artifacts have 5 PAF keys
    ```
    python3 check_artifact_pafs.py -d <ROOT-DIR> -v <VERSION>
    ```
    NOTE: the <ROOT-DIR> is optional and has a sensible default.
7. Rerun `make_artifacts -a` on any locations that do not have all 5 PAF keys. Note that you can do this one location at a time (e.g. `make_artifacts -l Alabama -a`) or you can run `make_artifacts -l all -a` again (but would again affect fairshare if there are lots of complete artifacts that would simply get skipped).

NOTE: If the PAFs cannot be appended to an artifact due to an inability to read the artifact (something about a lock file I was unable to find), I've found it useful to make a copy of the artifact and then run `make_artifacts -a` on the copy, e.g.
    ```
    >>> mv alabama.hdf alabama_copy.hdf
    >>> cp alabama_copy.hdf alabama.hdf
    >>> make_artifacts -l Alabama -a ...  # Confirm artifact is good
    >>> rm alabama_copy.hdf
    ```