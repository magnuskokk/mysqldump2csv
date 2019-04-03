#!/bin/bash

# Convert a single table MySQL dump to CSV
# Optionally remove duplicates

input="./data/test.sql"
output="./data/output2.csv"
uniq="./data/uniq2.csv"

function cleanup_files() {
    echo "Cleaning up files"
    rm -rf $output $uniq ./mysqldump2csv
}

function run() {
    go build
    
    # The CSV columns will be in the same order as specified here
    #pv $input | ./mysqldump2csv --columns "name,anothercolumn,created_at" > $output

    # give enough tmp space
    TMPDIR=/home/magnus sort -u $output -o $uniq
}

cleanup_files
time run
