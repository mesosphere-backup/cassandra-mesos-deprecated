#!/bin/bash
set -o errexit -o nounset -o pipefail

function main {(

    mvn clean package
    mkdir -p "$(pwd)/target/framework-package"
    ./package.bash download  # downloads cassandra
    ./package.bash _download https://downloads.mesosphere.io/java/jre-7u75-linux-x64.tar.gz "$(pwd)/target/framework-package/jre.tar.gz"
    export PORT0=18080
    export CASSANDRA_CLUSTER_NAME=dev-cluster
    export MESOS_ZK=zk://localhost:2181/mesos
    export CASSANDRA_ZK=zk://localhost:2181/cassandra-mesos
    export EXECUTOR_FILE_PATH=$(ls $(pwd)/cassandra-executor/target/cassandra-executor-*-jar-with-dependencies.jar)
    export JRE_FILE_PATH=$(pwd)/target/framework-package/jre.tar.gz
    export CASSANDRA_FILE_PATH=$(pwd)/target/framework-package/cassandra.tar.gz
    export CASSANDRA_NODE_COUNT=3
    export CASSANDRA_RESOURCE_CPU_CORES=1
    export CASSANDRA_RESOURCE_MEM_MB=512
    export CASSANDRA_RESOURCE_DISK_MB=1024
    export CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS=60
    export CASSANDRA_ZK_TIMEOUT_MS=10000

    java -cp $(ls $(pwd)/cassandra-framework/target/cassandra-framework-*-jar-with-dependencies.jar) io.mesosphere.mesos.frameworks.cassandra.Main

)}

main
