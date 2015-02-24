Cassandra Mesos Framework
=========================

# Configuration

All configuration is handled through environment variables (this lends itself well to being easy to configure marathon to run the framework).

## Framework Runtime Configuration

The following environment variables can be used to configure how the framework will operate.
```bash
# name of the cassandra cluster, this will be part of the framework name in Mesos
CASSANDRA_CLUSTER_NAME=dev-cluster

# Mesos ZooKeeper URL to locate leading master
MESOS_ZK=zk://localhost:2181/mesos

# ZooKeeper URL to be used to storm framework state
CASSANDRA_ZK=zk://localhost:2181/cassandra-mesos

# The number of nodes in the ring (default 3)
CASSANDRA_NODE_COUNT=3

# The number of CPU Cores for each Cassandra Node (default 2.0)
CASSANDRA_RESOURCE_CPU_CORES=2.0

# The number of Megabytes of RAM for each Cassandra Node (default 2048)
CASSANDRA_RESOURCE_MEM_MB=2048

# The number of Megabytes of Disk for each Cassandra Node (default 2048)
CASSANDRA_RESOURCE_DISK_MB=2048

# The number of seconds between each health check of the cassandra node (default 60)
CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS=60

# The number of seconds that should be used as the mesos framework timeout (default 604800 seconds / 7 days)
CASSANDRA_FAILOVER_TIMEOUT_SECONDS=604800

```

## Mesos Node Configuration

You will need to expand the port range managed by Mesos on each node so that it includes the standard cassandra ports.

This can be done by passing the following flag to the mesos-slave process:
```
--resources='ports:[31000-32000,7000-7001,7199-7199,9042-9042,9160-9160]'
```


## Build
The Cassandra Mesos Framework is a maven project with modules for the Framework, Scheduler, Executor and Model. Standard maven convention applies. The Framework and Executor are both built as `jar-with-dependencies` in addition to their standalone jar, so that they are easy to run and distribute.

### Setup maven toolchain for protoc

1. Download version 2.5.0 of protobuf [here](https://code.google.com/p/protobuf/downloads/list)
2. Install
  1. Linux (make sure g++ compiler is installed)
    1. Run the following commands to build protobuf

         ```
         tar xzf protobuf-2.5.0.tar.gz
         cd protobuf-2.5.0
         ./configure
         make
         ```

3. Create `~/.m2/toolchains.xml` with the following contents, Update `PROTOBUF_HOME` to match the directory you ran make in
  
  ```
  <?xml version="1.0" encoding="UTF-8"?>
  <toolchains>
    <toolchain>
      <type>protobuf</type>
      <provides>
        <version>2.5.0</version>
      </provides>
      <configuration>
        <protocExecutable>$PROTOBUF_HOME/src/protoc</protocExecutable>
      </configuration>
    </toolchain>
  </toolchains>
  ```

#### Resources
* https://developers.google.com/protocol-buffers/docs/downloads
* https://code.google.com/p/protobuf/downloads/list
* http://www.confusedcoders.com/random/how-to-install-protocol-buffer-2-5-0-on-ubuntu-13-04

### Running unit tests
```bash
mvn clean test
```

### Packaging artifacts
```bash
mvn clean package
```
If you want to skip running tests when developing locally and rebuilding the packages run the following:
```bash
mvn -Dmaven.test.skip=true package
```

## Framework Package
There is a packaging script `package.bash` that can be used to package the framework and create a marathon.json to run the framework on [Marathon](https://github.com/mesosphere/marathon)
```bash
./package.bash package
```

Generating the `marathon.json` is dependent upon the great JSON command line tool [jq](http://stedolan.github.io/jq/). jq allows for accurate JSON document manipulation using the pipelineing functionality it provides.  See `package.bash` for an example.

## Configuration

All configuration is handled via Environment Variables. This is the most portable and maintainable approach for configuring and running tasks via Marathon.

The following environment variables are required to run the Cassandra Mesos Framework
```bash
# The URL to zk where mesos is running
MESOS_ZK=zk://localhost:2181/mesos
```

## Development
For development of the Cassandra Framework you will need access to a Mesos Cluster (for help setting up a cluster see [Setting up a Mesosphere Cluster](http://mesosphere.com/docs/getting-started/datacenter/install/)).

*The main class of the framework, `io.mesosphere.mesos.frameworks.cassandra.Main`, can safely be ran from you IDE if that is your preferred development environment.*

Run `dev-run.bash` to startup the framework. You should then be able to see tasks being launched in your Mesos UI.

#### Configuration
The following environment variables (with example values) should be specified for local development:
```bash
# The port the http server used for serving assets to tasks should use.
# In normal operations this dynamic port will be provided by Marathon as part of the task that
# will run the framework
## Any port will do, just so long as it can be bound on your dev machine and is accessible from
## the mesos slaves.
PORT0=18080

# The file path to where the cassandra-executor jar-with-dependencies is on the local file system
# This file will be served by the built-in http server so that tasks will be able to easily access
# the jar.
EXECUTOR_FILE_PATH=${PROJECT_DIR}/cassandra-executor/target/cassandra-executor-0.1.0-SNAPSHOT-jar-with-dependencies.jar

# The file path to where a tar of the Oracle JDK version 7 update 75 is on the local file system.
# This file will be served by the build-in http server so that tasks will be able to easily access
# the jdk, and it doesn't have to be provided by the slave host.
JDK_FILE_PATH=${PROJECT_DIR}/target/framework-package/jdk.tar.gz

# The file path to where a tar of Apache Cassandra 2.1.2 is on the local file system.
# This file will be served by the build-in http server so that tasks will be able to easily access
# the cassandra server, and it doesn't have to be provided by the slave host.
CASSANDRA_FILE_PATH=${PROJECT_DIR}/target/framework-package/cassandra.tar.gz

```
