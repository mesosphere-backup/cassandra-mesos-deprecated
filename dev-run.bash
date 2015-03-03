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
    export EXECUTOR_FILE_PATH=$(ls $(pwd)/cassandra-executor/target/cassandra-executor-*-jar-with-dependencies.jar)
    export JRE_FILE_PATH=$(pwd)/target/framework-package/jre.tar.gz
    export CASSANDRA_FILE_PATH=$(pwd)/target/framework-package/cassandra.tar.gz
    cd cassandra-framework
    mvn exec:java -Dexec.mainClass="io.mesosphere.mesos.frameworks.cassandra.Main"

)}

main
