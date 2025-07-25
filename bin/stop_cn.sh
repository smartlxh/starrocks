#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#############################################################################
# This script is used to stop CN process
# Usage:
#     sh stop_cn.sh [option]
#
# Options:
#     -h, --help              display this usage only
#     -g, --graceful          send SIGTERM to CN process instead of SIGKILL
#
#############################################################################

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export STARROCKS_HOME=`cd "$curdir/.."; pwd`
# compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
export DORIS_HOME="$STARROCKS_HOME"
export PID_DIR=`cd "$curdir"; pwd`

source $STARROCKS_HOME/bin/common.sh

export_env_from_conf $STARROCKS_HOME/conf/cn.conf
export_shared_envvars

pidfile=$PID_DIR/cn.pid

SIG=9
TIME_OUT=-1

OPTS=$(getopt \
  -n $0 \
  -o gh \
  -l 'graceful' \
  -l 'timeout:' \
  -l 'help' \
  -- "$@")

eval set -- "$OPTS"

usage() {
    echo "
This script is used to stop CN process
Usage:
    ./stop_cn.sh [option]

Options:
    -h, --help              display this usage only
    -g, --graceful          send SIGTERM to CN process instead of SIGKILL
    --timeout               specify the timeout for graceful exit
"
    exit 0
}

while true; do
    case "$1" in
        --timeout) TIME_OUT=$2 ; shift 2 ;;
        --help|-h) usage ; shift ;;
        --graceful|-g) SIG=15 ; shift ;;
        --) shift ;  break ;;
    esac
done

# kill all python worker process
find "${UDF_RUNTIME_DIR}" -maxdepth 1 -name 'pyworker*' -print0 | while IFS= read -r -d $'\0' worker; do
    pid=$(echo "$worker" | sed -n 's/.*pyworker_\([0-9]*\).*/\1/p')
    if [[ ! -z "$pid" ]]; then
        kill -9 "$pid" > /dev/null
        rm -- "$worker"
    fi
done


if [ -f $pidfile ]; then
    pid=`cat $pidfile`
    pidcomm=`ps -p $pid -o comm=`
    if [ "starrocks_be"x != "$pidcomm"x ]; then
        echo "ERROR: pid process may not be CN"
        exit 1
    fi

    kill -${SIG} $pid > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        rm $pidfile
        exit 1
    fi

    # Waiting for a process to exit
    start_ts=$(date +%s)
    while kill -0 $pid > /dev/null 2>&1; do
        if [ $TIME_OUT -gt 0 ] && [ $(($(date +%s) - $start_ts)) -gt $TIME_OUT ]; then
            kill -9 $pid
            echo "graceful exit timeout, forced termination of the process"
            break
        else
            sleep 1
        fi
    done

    rm $pidfile
fi
