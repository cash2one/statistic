#!/usr/bin/env bash
# 本脚本在远程hadoop执行机上跑。
DATE=$1
PRODUCT=$2
MAPRED=$3
CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
PYTHON_BIN=${CURRENT_PATH}/python2.7/bin/python
CODE_PATH=${CURRENT_PATH}/code/

cd ${CODE_PATH}

${PYTHON_BIN} mapred.py ${DATE} ${PRODUCT} ${MAPRED}
