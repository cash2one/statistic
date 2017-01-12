#!/usr/bin/env bash
# 该脚本在本地跑
if [ $# -lt 5 ];then
    echo "./mapred_local_second.sh {hadoop} {product} {date} {input} {output}"
    exit 1
fi
HADOOP=$1
PRODUCT=$2
DATE=$3
INPUT=$4
OUTPUT=$5

REMOTE_CODE=/app/ps/spider/wdmqa/kgdc/code/

echo "=======prepare code======="
CODE_PATH=$(cd "$(dirname "$0")"; pwd)
set -x
cd ${CODE_PATH}
tar -czvf ../${PRODUCT}.tar.gz ./
set +x
CODE_TAR=${CODE_PATH}/../${PRODUCT}.tar.gz
REMOTE_TAR=${REMOTE_CODE}/${PRODUCT}.tar.gz
echo "tar to ${CODE_TAR}"

echo "=======clear remote hadoop path======="
set -x
${HADOOP} fs -mkdir ${REMOTE_CODE}
${HADOOP} fs -rmr ${OUTPUT}
${HADOOP} fs -rmr ${REMOTE_TAR}
set +x

echo "=======put file to hadoop path======="
set -x
${HADOOP} fs -put ${CODE_TAR} ${REMOTE_TAR}
set +x

echo "=======start hadoop job====="

set -x
${HADOOP} streaming \
 -D mapred.job.name="kgdc-hadoop-2" \
 -D mapred.job.map.capacity=1000 \
 -D mapred.map.tasks=100 \
 -D mapred.reduce.tasks=10 \
 -D mapred.job.priority=VERY_HIGH \
 -D mapreduce.input.fileinputformat.input.dir.recursive=true \
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7 \
 -cacheArchive ${REMOTE_TAR}#code \
 -file ./mapred.sh \
 -mapper "sh -x mapred.sh ${DATE} ${PRODUCT} mapper" \
 -reducer "sh -x mapred.sh ${DATE} ${PRODUCT} reducer" \
 -input ${INPUT} \
 -output ${OUTPUT}
set +x
echo "=======clear files======="
set -x
rm -f ${CODE_TAR}
set +x