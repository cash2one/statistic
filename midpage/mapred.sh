#!/usr/bin/env bash
if [ $# -lt 3 ];then
    echo "./mapred.sh {hadoop} {input} {output}"
    exit 1
fi
HADOOP=$1
INPUT=$2
OUTPUT=$3

echo "${HADOOP} hce \n
 -D mapred.map.tasks=100 \n
 -D mapred.reduce.tasks=10 \n
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7 \n
 -file mapred_user_path.py \n
 -mapper \"pyhce mapred_user_path.py\" \n
 -reducer \"pyhce mapred_user_path.py\" \n
 -input ${INPUT} \n
 -output ${OUTPUT}"

${HADOOP} hce \
 -D mapred.map.tasks=100 \
 -D mapred.reduce.tasks=10 \
 -cacheArchive /app/ps/spider/wdmqa/kgdc/tool/python2.7.6.tar.gz#python2.7 \
 -file mapred_user_path.py \
 -mapper "pyhce mapred_user_path.py" \
 -reducer "pyhce mapred_user_path.py" \
 -input ${INPUT} \
 -output ${OUTPUT}
 # -D mapred.task.hce.lib.ld.paths=${HADOOP_HOME}/libhdfs:${HADOOP_HOME}/libhce/lib:${JAVA_HOME}/jre/lib/amd64:${JAVA_HOME}/jre/lib/amd64/native_threads:${JAVA_HOME}/jre/lib/amd64/server:$LD_LIBRARY_PATH:python2.7/so \