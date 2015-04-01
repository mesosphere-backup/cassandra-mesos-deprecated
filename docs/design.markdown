Cassandra Mesos Framework
=========================


# Motivation

As the adoption of Apache Mesos expands, the demand to run Distributed Databases such as Cassandra expands as well. As part of running Cassandra on Mesos it is desirable that many operational tasks be automated by the framework rather than having to be done externally.

# Design

Develop an Apache Mesos Framework to support scheduling and running Cassandra nodes in a Mesos cluster.  This would involve developing a Mesos Scheduler, a custom Mesos Executor, integration with nodetool to allow for management of running nodes. Java will be used as the implementation language for the Framework Scheduler and Custom Executor because it is also the language of the Cassandra Project. Our plan is to contribute the integration code to the Cassandra Project so it works out of the box.To remove any dependency on external storage the Cassandra Framework will use the State Abstraction provided by Mesos to persist any meta-data that the framework scheduler will need to persist.

# Goal Scenario

An operator or application developer has decided that she wants a Cassandra ring, so a framework scheduler is started and registers with the Mesos Master. Once registration is successful the framework will begin the process of trying to bootstrap the initial seed nodes in succession. Once seed nodes are up and healthy the framework will begin expanding the cluster to the desired total number of nodes.  Once the nodes are up the framework will take care of running all periodic administration operations (nodetool repair, etc).  If a node were to be lost, the framework would first attempt to restart the node on the same host, and if unable to restart the node in a reasonable (1hr?) amount of time a new node would be started to take its place, and the framework would remove the lost node from the cluster.

# Components

## Mesos Framework

### Characteristics

1. Multiple instances of the framework can be run for multiple rings
1. Frameworks will re-register with Mesos when restarting if it was previously registered (Scheduler HA)
1. Fault Tolerant, to restart failed Cassandra node tasks and guarantee capacity
1. The framework will be self-contained and not depend on anything from the host system (past the linux kernel and standard libraries the jvm depends on)
1. Written for Java 7 update 75

## Mesos Scheduler

The Mesos scheduler is the component with the most high-level intelligence in the framework. It will need to possess the ability to bootstrap a ring and distribute the correct configuration to all subsequently started nodes. The Scheduler will also be responsible for orchestrating all tasks with regard to restarting nodes and triggering and monitoring periodic administrative tasks required by a node.

### Responsibilities

1. Bootstrapping a ring
1. Adding nodes to a ring
1. Restarting a node that has crashed[[11]](#resources)
1. Providing configuration to nodes
    1. Seed nodes
    1. Snitch Class
    1. JVM OPTS
    1. etc?
1. Scheduling and running administrative utilities
    1. nodetool repair
    1. others?
1. Provide visibility into operations planned or running
1. Handle node replacement which a node is super lost

### Characteristics

1. Registering with Mesos with a failover timeout
1. Supports framework authentication
1. Can be run in High-Availability (HA) mode
1. Declines offers to resources it doesn't need
1. Only use necessary fraction of offers
1. Deal with lost tasks
1. Does not rely on in-memory state
1. Verifies supported Mesos Version
1. Supports roles
1. Able to provide set of ports to be used by Nodes
    1. Initial implementation will be for a static set of ports with a potential for longer term dynamic port usage.
1. Written for Java 7 update 75

## Mesos Executor

The Custom Mesos Executor will be responsible for providing a high-fidelity means of integrating with nodetool and managing the running of a specific Cassandra node on a Mesos Slave.

### Responsibilities

1. Monitor health of running node
1. Use JMX Mbeans for interfacing with Cassandra Server Process
1. Communicate results of administrative actions via StatusUpdates to scheduler when necessary

### Characteristics

1. Does not rely on file system state outside sandbox
1. Pure libprocess communication with Scheduler leveraging StatusUpdate
1. Does not rely on running on a particular slave node
1. Data directories will be created and managed by Mesos leveraging the features provided in MESOS-1554 [[5]](#resources)

## Cassandra

### Nodes
1. Static Port configuration for all ports listed in [Resource 10](#resources) below

# Out of Scope for the First Version

* Support for reducing the size of a ring
* Data backup
* Automatic Upgrades
* Auto-scaling
* Performance Optimizations
* Dynamic Ports
* Multi-DataCenter support
* Support vertically scaling nodes (increase RAM or CPU resources for a node)
  * Depends on MESOS-1279 [[6]](#resources)
* Automatically scheduled `nodetool cleanup`
  * There are semantics of how a cluster is used that need to be taken into consideration when running cleanup. As such, we don't feel comfortable automatically running cleanup.

# Outstanding Questions


# Resources

1. http://www.datastax.com/documentation/cassandra/2.1/cassandra/operations/ops_add_node_to_cluster_t.html
2. https://wiki.apache.org/cassandra/Operations#Ring_management
3. https://issues.apache.org/jira/browse/CASSANDRA-8651
4. [MESOS-2018 Dynamic Reservations](https://issues.apache.org/jira/browse/MESOS-2018)
5. [MESOS-1554 Persistent resources support for storage-like services](https://issues.apache.org/jira/browse/MESOS-1554)
6. [MESOS-1279 Add resize task primitive](https://issues.apache.org/jira/browse/MESOS-1279)
7. https://github.com/tobert/cassandra-docker
8. Proof-of-concept Cassandra Framework: https://github.com/mesosphere/cassandra-mesos
9. HDFS Framework (another stateful service): https://github.com/mesosphere/hdfs
10. http://www.datastax.com/documentation/cassandra/2.0/cassandra/security/secureFireWall_r.html
11. http://www.datastax.com/documentation/cassandra/2.0/cassandra/operations/ops_replace_node_t.html
