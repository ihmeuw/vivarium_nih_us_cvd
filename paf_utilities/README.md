Artifact generation is currently a multi-step process as outlined in this README.


NOTE: This is the workflow implemented as of 2023-07-28.
NOTE: The following notes assume that the official artifact directory is located at /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/.
TODO: This workflow needs to be updated as it is refined (feature branches are merged, etc).


STEP 1: SET UP ENV
------------------
1. Set up the environment
    ```
    >>> conda create -n <ENV-NAME> python=3.10
    >>> conda activate <ENV-NAME>
    >>> cd <REPOS-DIR>/vivarium_nih_us_cvd; pip install -e .; git checkout main
    >>> cd <REPOS-DIR>/vivarium_public_health; pip install -e .; git checkout develop
    ```


STEP 2: GENERATE ARTIFACTS (WITHOUT PAFS)
-----------------------------------------
1. Create a new <VERSION> folder (e.g. v1-20230613) for the artifacts 
    ```
    >>> mkdir /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>
    ```
2. Request a cluster node appropriate to make artifacts for all locations. For reference, it took ~3.5 hours to generate the PAF-less artifact for Alabama the first time (and then MUCH less afterwards due to gbd call caching).
3. Run make_artifacts on all locations
    ```
    >>> make_artifacts -vvv --pdb -l all --ignore-pafs -o /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>
    ```


STEP 3: CALCULATE PAFS
----------------------
1. Request cluster resources appropriate for running the PAF simulations. This may take some trial and error! A good starting point is 3 threads, 11 hours, 50 GB
2. Make a `paf-calculations` subdirectory in the current artifact version folder (alongside the artifacts already generated) and set the directory permissions
    ```
    >>> mkdir -p /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>/paf-calculations
    >>> chmod 775 /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>/paf-calculations
    ```
3. (OPTIONAL) Update the cluster requests in ./paf_runner.py
4. Navigate to the repository root directory and run the ./calculate_pafs.sh script to launch a `psimulate run` command for all locations (defined in the shell script)
    ```
    >>> sh ./paf_utilities/calculate_pafs.sh run <VERSION> <ROOT-DIR>
    ```
    NOTE: the <ROOT-DIR> is optional and has a sensible default.
5. Review the output from ./calculate_pafs.sh and check if any states do not have 1000 rows generated.
6. If any locations lack 1000 draws, run the ./calculate_pafs.sh script to launch the `psimulate restart` command
    ```
    >>> sh ~/repos/vivarium_nih_us_cvd/paf_utilities/calculate_pafs.sh restart <VERSION> <ROOT-DIR>
    ```
7. Continue to restart the simulation until all locations have complete PAF outputs (1000 draws). This should hopefully not be more than once.

NOTE: ./calculate_pafs.sh calls ./check_completion.sh as the final step; you can always run ./check_completion.sh on its own.


STEP 4: ADD PAFS TO THE ARTIFACTS
---------------------------------
1. Append the new PAF data to the artifacts
    ```
    >>> make_artifacts --pdb -vvv -a -o /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/<VERSION>
    ```
2. Confirm that all artifacts have 5 PAF keys
    ```
    python3 check_artifact_pafs.py -d <ROOT-DIR> -v <VERSION>
    ```
    NOTE: the <ROOT-DIR> is optional and has a sensible default.
3. Rerun `make_artifacts -a` on any locations that do not have all 5 PAF keys. Note that you can do this one location at a time (e.g. `make_artifacts -l Alabama -a`) or you can run `make_artifacts -l all -a`

NOTE: If the PAFs cannot be appended to an artifact due to an inability to read the artifact (something about a lock file I was unable to find), I've found it useful to make a copy of the artifact and then run `make_artifacts -a` on the copy, e.g.
    ```
    >>> mv alabama.hdf alabama_copy.hdf
    >>> cp alabama_copy.hdf alabama.hdf
    >>> make_artifacts -l Alabama -a ...  # Confirm artifact is good
    >>> rm alabama_copy.hdf
    ```
