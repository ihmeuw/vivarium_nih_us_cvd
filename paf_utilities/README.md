Artifact generation is currently a multi-step process as outlined in this README.


NOTE: This is the workflow implemented as of 2023-10-19.
NOTE: This workflow needs to be updated as it is refined (feature branches are merged, etc).


VARIABLES USED IN DOC
---------------------
- <REPOS-DIR>: Directory where you clone your repos to
- <OUTPUT-ROOT-DIR>: Artifact output root directory (typically /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/)
- <VERSION>: Version subdirectory in <OUTPUT-ROOT-DIR>
- <ARTIFACT-ENV-NAME>: Environment with .[data] requirements installed
- <SIM-ENV-NAME>: Environment with .[dev] requirements installed


STEP 1: SET UP ENV
------------------
1. Set up the artifact environment
    ```
    >>> conda create -n <ARTIFACT-ENV-NAME> python=3.11
    >>> conda activate <ARTIFACT-ENV-NAME>
    >>> cd <REPOS-DIR>/vivarium_nih_us_cvd; pip install -e .[data]; git checkout main
    ```


STEP 2: GENERATE ARTIFACTS (WITHOUT PAFS)
-----------------------------------------
1. Create a new <VERSION> folder (e.g. v1-20230613) for the artifacts 
    ```
    >>> mkdir <OUTPUT-ROOT-DIR>/<VERSION>
    ```
2. Request a cluster node appropriate to make artifacts for all locations. For reference, it took ~3.5 hours to generate the PAF-less artifact for Alabama the first time (and then MUCH less afterwards due to gbd call caching).
3. Run make_artifacts on all locations
    ```
    >>> make_artifacts -vvv --pdb -l all --ignore-pafs -o <OUTPUT-ROOT-DIR>/<VERSION>
    ```


STEP 3: CALCULATE PAFS
----------------------
1. Set up the sim environment
    ```
    >>> conda create -n <SIM-ENV-NAME> python=3.11
    >>> conda activate <SIM-ENV-NAME>
    >>> cd <REPOS-DIR>/vivarium_nih_us_cvd; pip install -e .[dev]; git checkout main
    >>> conda install redis
    ```
2. Request cluster resources appropriate for running the PAF simulations. This may take some trial and error! A good starting point is 3 threads, 11 hours, 50 GB
3. Make a `paf-calculations` subdirectory in the current artifact version folder (alongside the artifacts already generated) and set the directory permissions
    ```
    >>> mkdir -p <OUTPUT-ROOT-DIR>/<VERSION>/paf-calculations
    >>> chmod 775 <OUTPUT-ROOT-DIR>/<VERSION>/paf-calculations
    ```
4. Run the PAF-generating simulations. There are two options to do so.
    A.1. (OPTIONAL) Update the cluster requests in ./paf_runner.py
    A.2. Navigate to the repository root directory and run the ./calculate_pafs.sh script to serially launch a `psimulate run` command for all locations (defined in the shell script)
        ```
        >>> sh ./paf_utilities/calculate_pafs.sh run <VERSION> <ROOT-DIR>
        ```
        NOTE: the <OUTPUT-ROOT-DIR> is optional but defaults to /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/.
    --- OR (new feature as of October 2023) ---
    B.1. Add all artifact paths to the paf_scenarios.yaml as a new branches key like:
        ```
        branches:
        - input_data:
            artifact_path:
                - '<OUTPUT-ROOT-DIR>/<VERSION>/alabama.hdf'
                - '<OUTPUT-ROOT-DIR>/<VERSION>/alaska.hdf'
                # ...
                - '<OUTPUT-ROOT-DIR>/<VERSION>/wyoming.hdf'
        ```
    B.2. Run PAF-generation sims:
        ```
        psimulate run <REPOS-DIR>/vivarium_nih_us_cvd/src/vivarium_nih_us_cvd/model_specifications/paf_calculation.yaml <REPOS-DIR>/vivarium_nih_us_cvd/src/vivarium_nih_us_cvd/model_specifications/branches/paf_scenarios.yaml -o <OUTPUT-ROOT-DIR>/<VERSION>/paf-calculations/ --max-workers <MAX-WORKERS> -vvv --pdb -m 20 -r 1:00:00 -P proj_simscience_prod
        ```
        where <MAX-WORKERS> is the maximum number of jobs you want to run at once (maybe 5000?)
5. Review the output from ./calculate_pafs.sh and check if any states do not have 1000 rows generated.
6. If any locations lack 1000 draws, run the ./calculate_pafs.sh script to launch the `psimulate restart` command
    ```
    >>> sh <REPOS-DIR>/vivarium_nih_us_cvd/paf_utilities/calculate_pafs.sh restart <VERSION> <OUTPUT-ROOT-DIR>
    ```
7. Continue to restart the simulation until all locations have complete PAF outputs (1000 draws). This should hopefully not be more than once.

NOTE: ./calculate_pafs.sh calls ./check_completion.sh as the final step; you can always run ./check_completion.sh on its own.


STEP 4: ADD PAFS TO THE ARTIFACTS
---------------------------------
1. Append the new PAF data to the artifacts
    ```
    >>> conda activate <ARTIFACT-ENV-NAME>
    >>> make_artifacts --pdb -vvval all -o <OUTPUT-ROOT-DIR>/<VERSION>
    ```
2. Confirm that all artifacts have the joint PAF key
    ```
    python3 check_artifact_pafs.py -d <OUTPUT-ROOT-DIR> -v <VERSION>
    ```
    NOTE: the <OUTPUT-ROOT-DIR> is optional but defaults to /mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations/.
3. Rerun `make_artifacts -a` on any locations that do not have all 5 PAF keys. Note that you can do this one location at a time (e.g. `make_artifacts -l Alabama -a`) or you can run `make_artifacts -l all -a`

NOTE: If the PAFs cannot be appended to an artifact due to an inability to read the artifact (something about a lock file I was unable to find), I've found it useful to make a copy of the artifact and then run `make_artifacts -a` on the copy, e.g.
    ```
    >>> mv alabama.hdf alabama_copy.hdf
    >>> cp alabama_copy.hdf alabama.hdf
    >>> make_artifacts -l Alabama -a ...  # Confirm artifact is good
    >>> rm alabama_copy.hdf
    ```
