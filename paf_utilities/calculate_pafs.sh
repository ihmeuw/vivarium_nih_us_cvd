#!/bin/bash

psimulate_cmd="$1"  # run or restart
artifact_ver="$2"
artifact_dir=${3-"/mnt/team/simulation_science/costeffectiveness/artifacts/vivarium_nih_us_cvd/51-locations"}

usage_comment="Usage: $0 <"run" or "restart"> <artifact version sub-folder> <artifact dir (optional)>"

if [[ "$psimulate_cmd" != "run" && "$psimulate_cmd" != "restart" ]]; then
    echo $usage_comment
    echo "Invalid argument. Please provide either 'run' or 'restart'; provided '$psimulate_cmd'"
    exit 1
fi

if [[ -z "$artifact_ver" ]]; then
    echo $usage_comment
    echo "Please provide an artifact version/sub-folder"
    exit 1
fi

cmd="./paf_utilities/paf_runner.sh '$psimulate_cmd' '$artifact_ver' '$artifact_dir'"
echo "Command to run for each location: $cmd"
sleep 10  # Useful to double-check inputs and ctrl-c if needed

locations=(
"alabama" \
"alaska" \
"arizona" \
"arkansas" \
"california" \
"colorado" \
"connecticut" \
"delaware" \
"district_of_columbia" \
"florida" \
"georgia" \
"hawaii" \
"idaho" \
"illinois" \
"indiana" \
"iowa" \
"kansas" \
"kentucky" \
"louisiana" \
"maine" \
"maryland" \
"massachusetts" \
"michigan" \
"minnesota" \
"mississippi" \
"missouri" \
"montana" \
"nebraska" \
"nevada" \
"new_hampshire" \
"new_jersey" \
"new_mexico" \
"new_york" \
"north_carolina" \
"north_dakota" \
"ohio" \
"oklahoma" \
"oregon" \
"pennsylvania" \
"rhode_island" \
"south_carolina" \
"south_dakota" \
"tennessee" \
"texas" \
"utah" \
"vermont" \
"virginia" \
"washington" \
"west_virginia" \
"wisconsin" \
"wyoming"
)

for location in "${locations[@]}"; do
    eval "$cmd '$location'"
done

wait

# Run rudimentary job completion checker
echo ""
echo "Simulations finished. Scanning main.log files and printing number of completed jobs:"
echo ""
sh ./paf_utilities/check_completion.sh $artifact_dir/$artifact_ver/paf_calculation
echo ""
echo "*** FINISHED ***"
