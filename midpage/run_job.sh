#!/usr/bin/env bash
# 该脚本在本地跑
if [ $# -lt 4 ];then
    echo "./mapred.sh {hadoop} {source} {input} {output}"
    exit 1
fi
HADOOP=$1
SOURCE=$2
INPUT=$3
OUTPUT=$4

REMOTE_CODE=/app/ps/spider/wdmqa/kgdc/code/${PRODUCT}

echo "=======prepare code======="
CODE_PATH=$(cd "$(dirname "$0")"; pwd)
cd ${CODE_PATH}
tar -czvf code.tar.gz ./
CODE_TAR=${CODE_PATH}/code.tar.gz
REMOTE_TAR=${REMOTE_CODE}/code.tar.gz
echo "tar to ${CODE_TAR}"

echo "=======clear remote hadoop path======="
${HADOOP} fs -mkdir ${REMOTE_CODE}
${HADOOP} fs -rmr ${OUTPUT}
${HADOOP} fs -rmr ${REMOTE_TAR}

echo "=======put file to hadoop path======="
${HADOOP} fs -put ${CODE_TAR} ${REMOTE_TAR}

echo "=======start hadoop job====="

echo "${HADOOP} streaming
 -D mapred.job.name="kgdc-hadoop"
 -D mapred.job.map.capacity=1000
 -D mapred.map.tasks=100
 -D mapred.reduce.tasks=10
 -D mapred.job.priority=VERY_HIGH
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7
 -cacheArchive ${REMOTE_TAR}#code
 -file ./start.sh
 -mapper \"sh -x start.sh map ${SOURCE}\"
 -reducer \"sh -x start.sh reduce ${SOURCE}\"
 -input ${INPUT}
 -output ${OUTPUT}"

${HADOOP} streaming \
 -D mapred.job.name="kgdc-hadoop" \
 -D mapred.job.map.capacity=1000 \
 -D mapred.map.tasks=100 \
 -D mapred.reduce.tasks=10 \
 -D mapred.job.priority=VERY_HIGH \
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7 \
 -cacheArchive ${REMOTE_TAR}#code \
 -file ./start.sh \
 -mapper "sh -x start.sh map ${SOURCE}" \
 -reducer "sh -x start.sh reduce ${SOURCE}" \
 -input ${INPUT} \
 -output ${OUTPUT}
 # -D mapred.task.hce.lib.ld.paths=${HADOOP_HOME}/libhdfs:${HADOOP_HOME}/libhce/lib:${JAVA_HOME}/jre/lib/amd64:${JAVA_HOME}/jre/lib/amd64/native_threads:${JAVA_HOME}/jre/lib/amd64/server:$LD_LIBRARY_PATH:python2.7/so \

echo "=======clear files======="
rm -f ${CODE_TAR}