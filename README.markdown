Cassandra Mesos Framework
=========================

# Design
A design document outlining what features and characteristics are being targeted by the Cassandra Mesos Framework can be found in the docs folder in [design.markdown](docs/design.markdown).

# Current Status

### Implemented
* The framework can register with Mesos providing a failover timeout so that if the framework disconnects from mesos tasks will continue to run.
* The number of nodes, amount of resources (cpu, ram, disk and ports) are all configurable and evaluated when resources offers from Mesos are taken into consideration.  
* cassandra.yaml and varaibles for cassandra-env.sh are provided by the scheduler as part of the task definition.
* Health checks are performed by the executor and results are sent back to the scheduler using messaging mechanisms provided by Mesos.
* The Framework can restart and reregister with mesos without killing tasks.
* The scheduler can send tasks to nodes to perform 'nodetool repair'
* The scheduler can send tasks to nodes to perform 'nodetool cleanup'
* The Framework can easily be launched by Marathon allowing for easy installation
* Repair Job coordination
* Cleanup Job coordination

### Near Term Tasks
* Integration tests
* Create stress tests to try and simulate real world workloads and to identify bugs in fault tolerance handling
* Replace Node
* Rolling restart
* Improved heap calculation to allow for memory mapped files

# Running the Framework

Currently the recommended way to run the Cassandra Mesos Framework is via Marathon. A `marathon.json` from the latest build can be found [here](https://teamcity.mesosphere.io/guestAuth/repository/download/Oss_Mesos_Cassandra_CassandraFramework/.lastSuccessful/marathon.json).

Once you've downloaded the marathon.json update the `MESOS_ZK` url and any other parameters you would like to change. Then POST the marathon.json to your marathon instance and the framework will boostrap itself.

## Mesos Node Configuration

You will need to expand the port range managed by Mesos on each node so that it includes the standard cassandra ports.

This can be done by passing the following flag to the mesos-slave process:
```
--resources='ports:[31000-32000,7000-7001,7199-7199,9042-9042,9160-9160]'
```

# Configuration

All configuration is handled through environment variables (this lends itself well to being easy to configure marathon to run the framework).

## Framework Runtime Configuration

The following environment variables can be used to configure how the framework will operate.
```bash
# name of the cassandra cluster, this will be part of the framework name in Mesos
CASSANDRA_CLUSTER_NAME=dev-cluster

# Mesos ZooKeeper URL to locate leading master
MESOS_ZK=zk://localhost:2181/mesos

# ZooKeeper URL to be used to store framework state
CASSANDRA_ZK=zk://localhost:2181/cassandra-mesos

# The number of nodes in the cluster (default 3)
CASSANDRA_NODE_COUNT=3

# The number of seed nodes in the cluster (default 2)
# set this to 1, if you only want to spawn one node
CASSANDRA_SEED_COUNT=2

# The number of CPU Cores for each Cassandra Node (default 2.0)
CASSANDRA_RESOURCE_CPU_CORES=2.0

# The number of Megabytes of RAM for each Cassandra Node (default 2048)
CASSANDRA_RESOURCE_MEM_MB=2048

# The number of Megabytes of Disk for each Cassandra Node (default 2048)
CASSANDRA_RESOURCE_DISK_MB=2048

# The number of seconds between each health check of the cassandra node (default 60)
CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS=60

# The default bootstrap grace time - the minimum interval between two node starts
# You may set this to a lower value in pure local development environments.
CASSANDRA_BOOTSTRAP_GRACE_TIME_SECONDS=120

# The number of seconds that should be used as the mesos framework timeout (default 604800 seconds / 7 days)
CASSANDRA_FAILOVER_TIMEOUT_SECONDS=604800

# The mesos role to used to reserve resources (default *). If this is set, the framework only accepts offers that have resources for that role.
CASSANDRA_FRAMEWORK_MESOS_ROLE=*
```


## Build
The Cassandra Mesos Framework is a maven project with modules for the Framework, Scheduler, Executor and Model. Standard maven convention applies. The Framework and Executor are both built as `jar-with-dependencies` in addition to their standalone jar, so that they are easy to run and distribute.

### Install Maven
The Cassandra Mesos Framework requires an install of Maven 3.x.x.

## Cassandra memory usage

Memory used by Cassandra can be roughly categorized into:

* Java heap memory. The amount of memory used by the Java VM for heap memory.
* Off heap memory. Off heap is used for several reasons by Cassandra:
     * **index-summary** (default: 5% of the heap size)
       configured in `cassandra.yaml` - see `index_summary_capacity_in_mb`
       default to 5% of the heap size (may exceed)
     * **key-cache** (default: 5% of the heap size)
       configured in `cassandra.yaml` - see `key_cache_size_in_mb`
       default to 5% of the heap size
     * **row-cache** (default: off)
       configured in `cassandra.yaml` - see `row_cache_size_in_mb` (must be explicitly enabled in taskEnv)
       default to 0
     * **counter-cache** (default: min(2.5% of Heap (in MB), 50MB))
       configured in `cassandra.yaml` - see `counter_cache_size_in_mb`
       default: min(2.5% of Heap (in MB), 50MB) ; 0 means no cache
     * **memtables** (default on-heap)
       configured in `cassandra.yaml` - see `file_cache_size_in_mb`
       default to the smaller of 1/4 of heap or 512MB
     * **file-cache** (default: min(25% of Heap (in MB), 512MB))
       configured in `cassandra.yaml` - see `file_cache_size_in_mb`
       default to the smaller of 1/4 of heap or 512MB
     * overhead during flushes/compactions/cleanup
       implicitly defined by workload
* OS buffer cache. The amount of (provisioned) memory reserved for the operating system for disk block buffers.

The default configuration simply assumes that you need as much off-heap memory than Java heap memory.
It basically divides the provisioned amount of memory by 2 and assigns it to the Java heap.

A good planned production system is sized to meet its workload requirements. That does mean proper values for
Cassandra process environment, `cassandra.yaml` and memory sizing.

You should not run Cassandra (even in test environments) with less than 4 GB configured in `memMb`.
A recommended minimum value for `memMb` is 16GB. In times where RAM is getting cheaper, provision as much as you can
afford - with 8 to 16 GB for `memJavaHeapMb`. Remember to figure out the really required numbers in load and
stress tests with your application.


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

# The file path to where a tar of the Oracle JRE version 7 update 75 is on the local file system.
# This file will be served by the build-in http server so that tasks will be able to easily access
# the jre, and it doesn't have to be provided by the slave host.
JRE_FILE_PATH=${PROJECT_DIR}/target/framework-package/jdk.tar.gz

# The file path to where a tar of Apache Cassandra 2.1.2 is on the local file system.
# This file will be served by the build-in http server so that tasks will be able to easily access
# the cassandra server, and it doesn't have to be provided by the slave host.
CASSANDRA_FILE_PATH=${PROJECT_DIR}/target/framework-package/cassandra.tar.gz

```

## Using Cassandra tools

Support for standard command line tools delivered with Apache Cassandra against clusters running on
Apache Mesos is provided using the provided shell scripts starting with `com-`. These tools use the
_live nodes API_ discussed below.

These are:

* `com-cqlsh` to invoke `cqlsh` without bothering about actual endpoints. It connects to any (random)
  live Cassandra node.
* `com-nodetool` to invoke `nodetool` without bothering about actual endpoints. It connects to any (random)
  live Cassandra node.
* `com-stress` to invoke `cassandra-stress` without bothering about actual endpoints. It connects to any (random)
  live Cassandra node.

All these tools are configured using environment variables and special command line options. These command
line options must be specified directly after the command name.
 
Environment variables:

* `CASSANDRA_HOME` path to where your local unpacked Apache Cassandra distribution lives. Defaults to `.`
* `API_HOST` host name where the Cassandra-on-Mesos scheduler is running. Defaults to `127.0.0.1`
* `API_PORT` port on which the Cassandra-on-Mesos scheduler is listening. Defaults to `18080`

Command line options:
* `--limit N` the number of live nodes to use. Has no effect for cqlsh or nodetool.

## Live Cassandra nodes API

This framework provides API endpoints for most tools. All you need is `curl` and the hostname/IP of
the node running the scheduler of this framework.

There are also two endpoints - one returns a simple JSON structure and one just plain ASCII with the native port
in the first line and node IP addresses on each following line. The number as the last part of the path determines
the number of nodes you'd like to have.

Example for `json` endpoint:
```
curl -s http://192.168.5.101:18080/live-nodes/json/2
{
  "nativePort" : 9042,
  "rpcPort" : 9160,
  "jmxPort" : 7199,
  "liveNodes" : [ "127.0.0.2", "127.0.0.1" ]
}
```

Example for `ascii` endpoint:
```
curl -s http://192.168.5.101:18080/live-nodes/ascii/2
9042
127.0.0.1
127.0.0.2
```

Note that the implementation does a best-effort approach to return random nodes.
