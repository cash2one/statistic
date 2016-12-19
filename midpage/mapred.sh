#!/usr/bin/env bash
# 本脚本在远程hadoop执行机上跑。
TYPE=$1
SOURCE=$2
CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
PYTHON_BIN=${CURRENT_PATH}/python2.7/bin/python
CODE_PATH=${CURRENT_PATH}/code/

cd ${CODE_PATH}

${PYTHON_BIN} mapred.py ${TYPE} ${SOURCE}
