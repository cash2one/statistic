#!/usr/bin/env bash
# 该脚本在本地跑
if [ $# -lt 3 ];then
    echo "./run_distcp.sh {hadoop} {source} {destination}"
    exit 1
fi
HADOOP=$1
SRC=$2
DES=$3
set -x
${HADOOP} fs -mkdir ${DES}
${HADOOP} distcp \
 -D mapred.job.name="kgdc-distcp" \
 -D mapred.job.priority=VERY_HIGH \
 -update "${SRC}" "${DES}"
set +x