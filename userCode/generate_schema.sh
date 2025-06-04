#!/bin/sh

# script for generating the python type hints from the ODM2 vocabulary json

set -e

cd $(dirname $0) || exit 1

curl http://vocabulary.odm2.org/api/v1/variablename/?format=json | \
jq -r '.objects[].resource_uri' | \
awk -F'/' '{print "\"" $(NF-1) "\","}' | \
sed '$ s/,$//' | \
awk 'BEGIN {print "## THIS FILE IS GENERATED - DO NOT EDIT\nfrom typing import Literal\n\nResourceURI = Literal["} {print "    " $0} END {print "]"}' \
>> schema.py
