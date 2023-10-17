
    export VIVARIUM_LOGGING_DIRECTORY=/mnt/team/simulation_science/priv/engineering/vivarium_nih_us_cvd/19_adjusting_therapeutic_inertia/alabama/2023_10_16_16_12_21/logs/2023_10_16_16_12_21_run/worker_logs
    export PYTHONPATH=/mnt/team/simulation_science/priv/engineering/vivarium_nih_us_cvd/19_adjusting_therapeutic_inertia/alabama/2023_10_16_16_12_21:$PYTHONPATH

    /ihme/homes/sbachmei/miniconda3/envs/cvd/bin/rq worker -c settings         --name ${SLURM_ARRAY_JOB_ID}.${SLURM_ARRAY_TASK_ID}         --burst         -w "vivarium_cluster_tools.psimulate.worker.core._ResilientWorker"         --exception-handler "vivarium_cluster_tools.psimulate.worker.core._retry_handler" vivarium

    