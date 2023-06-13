#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <parent_directory>"
    exit 1
fi

parent_dir="$1"

# Loop through each subfolder in the parent directory
for subfolder in "$parent_dir"/*/; do
    subfolder_name=$(basename "$subfolder")
    # echo "Folder: $subfolder_name"

    # Find all "main.log" files recursively within the subfolder, sort them by filename
    log_files=$(find "$subfolder" -name "main.log" -type f -print 2>/dev/null | sort)

    # Find the most recent log file within the subfolder
    latest_log=$(echo "$log_files" | tail -n 1)

    # If a log file is found, perform grep on it and extract the matching pattern
    if [[ -n $latest_log ]]; then
        # grep_result=$(grep -o "with .* rows" "$latest_log")
        grep_result=$(grep -o -P "(?<=with ).*(?= rows)|No jobs to run" "$latest_log")
        if [[ -n $grep_result ]]; then
            echo "$subfolder_name: $grep_result"
        else
            echo "$subfolder_name: BAD"
        fi
    fi

done
