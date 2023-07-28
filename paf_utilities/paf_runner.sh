#!/bin/bash

queue="all.q"
runtime="00:20:00"
mem=3
project="proj_simscience_prod"


psimulate_cmd="$1"
artifact_ver="$2"
artifact_dir="$3"
location="$4"


outputdir="$artifact_dir/$artifact_ver"
if [ "$psimulate_cmd" == "run" ]; then
    cmd="psimulate run src/vivarium_nih_us_cvd/model_specifications/paf_calculation.yaml src/vivarium_nih_us_cvd/model_specifications/branches/paf_scenarios.yaml -i $outputdir/$location.hdf -o $outputdir/paf-calculations/ -m $mem -r $runtime -q $queue -P $project"
elif [ "$psimulate_cmd" == "restart" ]; then
    results_root=$outputdir/paf-calculations/$location/
    # Find all "main.log" files recursively within the subfolder, sort them by filename
    output_files=$(find "$results_root" -name "output.hdf" -type f -print 2>/dev/null | sort)
    latest_output=$(echo "$output_files" | tail -n 1)
    latest_results_root=$(dirname $latest_output)
    if [[ -n $latest_output ]]; then
        cmd="psimulate restart $latest_results_root -m $mem -r $runtime -q $queue -P $project --pdb"
    fi
else
    echo "Must pass in 'run' or 'restart' only, provided '$psimulate_cmd'"
    exit 1
fi

echo ""
echo "call: $cmd"
if eval "$cmd"; then
    echo "*** FINISHED ($location) ***"
else
    echo "*** ERROR ($location) - moving on ***"
fi

wait