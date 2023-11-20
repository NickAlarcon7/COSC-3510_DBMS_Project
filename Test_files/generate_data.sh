#!/bin/bash

# Function to create a CSV file with a specific pattern
generate_csv() {
    local filename=$1
    local count=$2
    local pattern=$3

    # Add column headers
    echo "column1,column2" > "$filename"

    # Generate data
    for ((i=1; i<=count; i++)); do
        if [ "$pattern" == "i-i" ]; then
            echo "$i,$i"
        elif [ "$pattern" == "i-1" ]; then
            echo "$i,1"
        fi
    done >> "$filename"
}

# Generate the required CSV files
generate_csv "Rel-i-i-1000.csv" 1000 "i-i"
generate_csv "Rel-i-1-1000.csv" 1000 "i-1"
generate_csv "Rel-i-i-10000.csv" 10000 "i-i"
generate_csv "Rel-i-1-10000.csv" 10000 "i-1"

echo "CSV files generated."
