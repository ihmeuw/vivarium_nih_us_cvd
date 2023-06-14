#!/bin/bash

psimulate_cmd="$1"  # run or restart

if [[ -z "$psimulate_cmd" || ("$psimulate_cmd" != "run" && "$psimulate_cmd" != "restart") ]]; then
    echo "Invalid argument. Please provide either 'run' or 'restart'; provided '$psimulate_cmd'"
    exit 1
fi

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
    cmd="./paf_utilities/paf_runner.sh '$psimulate_cmd' '$location'"
    eval "$cmd"
done
