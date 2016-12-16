#!/bin/bash

START=$(date +%s)

python ./dbFix.py 2> error_logs.txt 1> std_logs.txt

END=$(date +%s)


TIME=$((END - START))
T=$(date -d@$TIME -u +%H:%M:%S)
echo Time: $T Seconds: $TIME


