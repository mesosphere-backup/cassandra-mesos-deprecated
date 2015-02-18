#!/bin/bash
set -o errexit -o nounset -o pipefail

function main {(

#    mvn clean install
#    ./package.bash download
    export PORT0=18080
    export CASSANDRA_CLUSTER_NAME=dev-cluster
    export MESOS_ZK=zk://localhost:2181/mesos
    export EXECUTOR_FILE_PATH=$(ls $(pwd)/cassandra-executor/target/cassandra-executor-*-jar-with-dependencies.jar)
    export JDK_FILE_PATH=$(pwd)/target/framework-package/jdk.tar.gz
    export CASSANDRA_FILE_PATH=$(pwd)/target/framework-package/cassandra.tar.gz
    cd cassandra-framework
    mvn exec:java -Dexec.mainClass="io.mesosphere.mesos.frameworks.cassandra.Main"

)}

main
