#!/bin/bash
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

set -o errexit -o nounset -o pipefail

function maven {(
    mvn clean package
)}

function download {(
    wget --progress=dot -e dotbytes=1M -O "$(pwd)/cassandra-mesos-dist/target/tarball/jre.tar.gz" https://downloads.mesosphere.io/java/jre-7u75-linux-x64.tar.gz
)}

function execute {(
    export PORT0=18080
    export CASSANDRA_CLUSTER_NAME=dev-cluster
    export MESOS_ZK=zk://localhost:2181/mesos
    export CASSANDRA_ZK=zk://localhost:2181/cassandra-mesos
    export EXECUTOR_FILE_PATH=$(ls $(pwd)/cassandra-mesos-dist/target/tarball/cassandra-mesos-executor.jar)
    export JRE_FILE_PATH=$(pwd)/cassandra-mesos-dist/target/tarball/jre.tar.gz
    export CASSANDRA_FILE_PATH=$(ls $(pwd)/cassandra-mesos-dist/target/tarball/apache-cassandra-*.tar.gz)
    export CASSANDRA_NODE_COUNT=3
    export CASSANDRA_RESOURCE_CPU_CORES=1
    export CASSANDRA_RESOURCE_MEM_MB=512
    export CASSANDRA_RESOURCE_DISK_MB=1024
    export CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS=60
    export CASSANDRA_ZK_TIMEOUT_MS=10000

    java -cp $(pwd)/cassandra-mesos-dist/target/tarball/cassandra-mesos-framework.jar io.mesosphere.mesos.frameworks.cassandra.framework.Main
)}

function main {(
    maven
    download
    execute
)}

if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main
fi
