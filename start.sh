#!/usr/bin/env bash

DATE=$(date +%Y%m%d -d "-1 days")
python main.py midpage_createdb ${DATE}
python main.py midpage_analysis ${DATE} & 1>>log/midpage_period.log 2>&1
python main.py midpage_mapred ${DATE} & 1>>log/midpage_mapred.log 2>&1