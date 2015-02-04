Cassandra Mesos Framework
=========================

## Build
The Cassandra Mesos Framework is a maven project with modules for the Framework, Scheduler, Executor and Model. Standard maven convention applies. The Framework and Executor are both built as `jar-with-dependencies` in addition to their standalone jar, so that they are easy to run and distribute.

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
# name of the cassandra cluster, this will be part of the framework name in Mesos
CASSANDRA_CLUSTER_NAME=dev-cluster

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
