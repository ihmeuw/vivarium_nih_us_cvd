#!/bin/bash

psimulate_cmd="$1"  # run or restart

if [[ -z "$psimulate_cmd" || ("$psimulate_cmd" != "run" && "$psimulate_cmd" != "restart") ]]; then
    echo "Invalid argument. Please provide either 'run' or 'restart'; provided '$psimulate_cmd'"
    exit 1
fi

locations=(
# "alabama" \
# "alaska" \
# "arizona" \
# "arkansas" \
# "california" \
# "colorado" \
# "connecticut" \
# "celaware" \
# "district_of_columbia" \
# "florida" \
# "georgia" \
# "hawaii" \
# "idaho" \
# "illinois" \
"indiana" \
# "iowa" \
# "kansas" \
# "kentucky" \
# "louisiana" \
# "maine" \
# "maryland" \
# "massachusetts" \
# "michigan" \
# "minnesota" \
# "mississippi" \
# "missouri" \
# "montana" \
# "nebraska" \
# "nevada" \
# "new_hampshire" \
# "new_jersey" \
# "new_mexico" \
# "new_york" \
# "north_carolina" \
# "north_dakota" \
# "ohio" \
# "oklahoma" \
# "oregon" \
"pennsylvania" \
# "rhode_island" \
# "south_carolina" \
# "south_dakota" \
# "tennessee" \
# "texas" \
# "utah" \
# "vermont" \
# "virginia" \
# "washington" \
# "west_virginia" \
# "wisconsin" \
# "wyoming"
)

for location in "${locations[@]}"; do
    cmd="./paf_utilities/paf_runner.sh '$psimulate_cmd' '$location'"
    eval "$cmd"
done

# arkansas: 128  # MISSING 1
# florida: BAD  # MISSING 3
# hawaii: BAD  # MISSING 1
# idaho: BAD  # missing 1
# illinois: BAD  # missing 1
# indiana: BAD  # missing 1
# iowa: BAD  # missing 1
# missouri: BAD  # missing 1
# nebraska: BAD  # missing 1
# new_jersey: BAD  # missing 2
# pennsylvania: BAD  # missing 1