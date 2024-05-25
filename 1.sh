#!/bin/bash

# Function to run databricks configure in the directory containing databricks.yml
process_directory() {
    local dir="$1"
    echo "Configuring Databricks in $dir"
    (cd "$dir" && databricks bundle validate -p test -t dev && databricks bundle deploy -p test -t dev)
}

# Export the function so it's available to find
export -f process_directory

# Find directories containing databricks.yml and process them
find . -type f -name 'databricks.yml' -execdir bash -c 'process_directory "$(pwd)"' \;

# Alternatively, without exporting function
# find . -type f -name 'databricks.yml' -print0 | while IFS= read -r -d '' file; do
#     dir=$(dirname "$file")
#     echo "Configuring Databricks in $dir"
#     (cd "$dir" && databricks configure)
# done
