#!/bin/sh
#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

wd=`pwd`
cd `dirname $0`
LOADTEST_DIR=`pwd`
cd ${wd}

PROFILE="${LOADTEST_DIR}/cqlstress-example.yaml"
OPS="ops(insert=1,simple1=1)"

LOGFILE=""
LOGARGS=""
DEFAULT_OPTS="-mode native cql3 -rate threads>=50"
LIMIT=9999
DURATION=""
N=""

# Hint to work with cassandra-stress:
# There's no "global" help page that contains everything.
# You can get more descent information if you query on each individual command and option. Examples
#   tools/bin/cassandra-stress help -rate       to get help on -rate option
#   tools/bin/cassandra-stress help user        to get help on user command

while true ; do
    if [ "$1" = "--limit" ] ; then
        shift
        LIMIT=$1
        shift
    elif [ "$1" = "--duration" ] ; then
        shift
        DURATION="duration=$1"
        shift
    elif [ "$1" = "--n" ] ; then
        shift
        N="n=$1"
        shift
    elif [ "$1" = "--profile" ] ; then
        shift
        if [ -f $1 ] ; then
            PROFILE=$1
        else
            PROFILE=${LOADTEST_DIR}/$1
        fi
        shift
    elif [ "$1" = "--ops" ] ; then
        shift
        OPS="ops($1)"
        shift
    elif [ "$1" = "--logdir" ] ; then
        shift
        LOGDIR="$1"
        shift
    elif [ "$1" = "--no-default-opts" ] ; then
        shift
        DEFAULT_OPTS=""
    else
        break
    fi
done

if [ -z ${LOGDIR} ] ; then
    LOGDIR=`pwd`/loadtest-`date '+%Y-%m-%d-%H-%M-%S'`
fi
mkdir -p ${LOGDIR}

if [ -z ${LOGFILE} ] ; then
    LOGFILE="${LOGDIR}/cassandra-stress.log"
fi
if [ -z ${LOGARGS} ] ; then
    LOGARGS="-log file=${LOGFILE}"
fi

MY_LOGFILE=${LOGDIR}/cluster-loadtest.log

cat >> ${MY_LOGFILE} << !
Starting cluster-loadtest.sh on `date`

Invoking com-stress as:
${LOADTEST_DIR}/../shell-scripts/bin/com-stress \\
    --limit ${LIMIT} \\
    user "profile=${PROFILE}" "${DURATION}" "${N}" \\
    "${OPS}" \\
    "${DEFAULT_OPTS}" \\
    "${LOGARGS}" \\
    "$@"

Measuring output of cassandra-stress goes to
    ${LOGFILE}

===============================================================

!


echo "cassandra-stress uses log file ${LOGFILE}"

${LOADTEST_DIR}/../shell-scripts/bin/com-stress \
    --limit ${LIMIT} \
    user "profile=${PROFILE}" "${DURATION}" "${N}" \
    "${OPS}" \
    "${DEFAULT_OPTS}" \
    "${LOGARGS}" \
    "$@" 2>&1 | tee -a ${MY_LOGFILE}

ec=$?

cat >> ${MY_LOGFILE} << !

===============================================================

cluster-loadtest.sh finished on `date`

Exit code: ${ec}
!

${LOADTEST_DIR}/../shell-scripts/bin/com-qa-report ${LOGDIR}
