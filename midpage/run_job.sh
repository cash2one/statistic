#!/usr/bin/env bash
# 该脚本在本地跑
if [ $# -lt 4 ];then
    echo "./run_job.sh {hadoop} {source} {input} {output}"
    exit 1
fi
HADOOP=$1
SOURCE=$2
INPUT=$3
OUTPUT=$4

REMOTE_CODE=/app/ps/spider/wdmqa/kgdc/code/${SOURCE}

echo "=======prepare code======="
CODE_PATH=$(cd "$(dirname "$0")"; pwd)
set -x
cd ${CODE_PATH}
tar -czvf ../code.tar.gz ./
set +x
CODE_TAR=${CODE_PATH}/../code.tar.gz
REMOTE_TAR=${REMOTE_CODE}/code.tar.gz
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
 -D mapred.job.name="kgdc-hadoop" \
 -D mapred.job.map.capacity=1000 \
 -D mapred.map.tasks=100 \
 -D mapred.reduce.tasks=20 \
 -D mapred.job.priority=VERY_HIGH \
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7 \
 -cacheArchive ${REMOTE_TAR}#code \
 -file ./mapred.sh \
 -mapper "sh -x mapred.sh map ${SOURCE}" \
 -reducer "sh -x mapred.sh reduce ${SOURCE}" \
 -input ${INPUT} \
 -output ${OUTPUT}
 # -D mapred.task.hce.lib.ld.paths=${HADOOP_HOME}/libhdfs:${HADOOP_HOME}/libhce/lib:${JAVA_HOME}/jre/lib/amd64:${JAVA_HOME}/jre/lib/amd64/native_threads:${JAVA_HOME}/jre/lib/amd64/server:$LD_LIBRARY_PATH:python2.7/so \
set +x
echo "=======clear files======="
set -x
rm -f ${CODE_TAR}
set +x