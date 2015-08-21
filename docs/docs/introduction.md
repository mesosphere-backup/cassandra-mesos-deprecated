---
title: Introduction to Cassandra-Meso
---

------------

**DISCLAIMER**
_This is a very early version of Cassandra-Mesos framework. This document, code behavior, and anything else may change without notice and/or break older installations._

------------

# Introduction to Cassandra-Mesos

There are some differences when running [Apache Cassandra] on [Apache Mesos] versus natively. This document gives experienced Cassandra users a high level overview of these differences.

## Apache Mesos

Apache Mesos is a cluster resource manager and scheduler that provides an easy and robust abstraction to run services in your datacenter.

Behind the scenes, Apache Mesos uses [Apache ZooKeeper], a distributed configuration store to persist its state. The "state" of Mesos includes everything from configuration to the status and load of individual Mesos nodes, as well as frameworks like Cassandra-Mesos.

For common Mesos terminology, see the [Mesosphere Glossary](http://mesosphere.com/docs/reference/glossary/).

## Standard Apache Cassandra Installation

The standard way of installing Cassandra involves installing and configuring the operating system, JVM and Cassandra manually on each node in your cluster.

# Initial Cassandra-Mesos setup

If you want to try Cassandra-Mesos on a single development machine, see the [Getting Started]({{ site.baseurl }}/docs/) page.

# Prerequisites

* [Apache Mesos]: Mesos and ZooKeeper in a HA configuration
* Servers with *local storage* (for Cassandra) configured and running as Mesos slaves
* Knowledge of how many resources each of your Cassandra nodes require (RAM, disk space, CPU)

# Configuration

The Cassandra-Mesos framework requires these things before you get started:

* Number of Cassandra nodes
* Number of seed nodes
* Number of CPU cores per node
* Amount of RAM per node (see below)
* Amount of disks and space per disk (see below)
* Name of the cluster
* Name of Mesos role
* Additional, non-default configuration in `cassandra.yaml`
* Name of the snitch, name of the datacenter and name of the rack (see below)

Configuring RAM requires the total amount
of RAM (used by Mesos for provisioning) and the heap size of the Java VM. Off-heap space that is used by Cassandra and
operating system block level cache has to fit into the gap between these two parameters.

Mesos acquires a workspace (a directory) per executor. The workspace is usually recreated when the
executor starts, which means the executor gets an empty directory on each start. Because this is not a good
behavior when using databases (Cassandra is a database), Mesos 0.22 introduces the ability to allocate
multiple permanent disk locations (*not* ephemeral) that can be reused when the executor restarts.

Apache Mesos is not aware of the different data centers or racks on its own like Apache Cassandra. Currently, 
Cassandra-Mesos does not support multiple datacenters or racks. To enable this feature in  a future release of
Cassandra-Mesos, we've chosen to use the `GossipingPropertyFileSnitch` by default and to make datacenter and
rack name configurable.

In summary, the Cassandra-Mesos framework performs the following tasks:

* Deploys JRE to nodes. However, you must provide the framework a location where you provide a copy of the Oracle
  JRE due to legal/licensing restrictions.
* Deploys Apache Cassandra to nodes
* Configures Apache Cassandra (`cassandra.yaml` + `cassandra-rackdc.properties`)

# CLI tools

## Cassandra tool extensions

Apache Cassandra ships with the useful tools `cqlsh`, `nodetool`, and `cassandra-stress`. These tools usually require you
to pass in the nodes that you want to connect to. This becomes a bit complicated with Mesos because Cassandra nodes might
be running on a node with an arbitrary IP address.

Cassandra-Mesos provides two features to solve this problem:

* REST API endpoints to discover live Cassandra nodes
* Shell scripts that use this API

To use these features, you need these tools:
 
* An unpacked copy of Apache Cassandra
* The set of Cassandra-Mesos shell scripts
* The IP/hostname of the host running the scheduler (Mesos-DNS makes this task easier)

To run the above tools do the following:

* `cqlsh` - use `com-cqlsh` instead. You can pass all `cqlsh` options to `com-cqlsh`, except host and port.
* `nodetool` - use `com-nodetool` instead. You can pass all `nodetool` options to `com-nodetool`, except host and port.
  **Important JMX security issue** since version 2.1.4 (and 2.0.14) Cassandra no longer exposes the JMX port to all
  interfaces but only to the loopback address. See [readme](https://github.com/mesosphere/cassandra-mesos/blob/master/README.md) for details.
* `cassandra-stress` - use `com-stress` instead. You can pass all `cassandra-stress` options to `com-stress`,except host and port.

The REST API endpoint chooses the nodes from the set of live nodes randomly. If you use `nodetool`
against a specific node, you must find that node's IP address and use `nodetool` as usual.

## Other Cassandra tools

Running Cassandra tools like the many `sstable*` tools in `bin` and `tools/bin` is currently more difficult.
You must log onto a slave node, `cd` to the sandbox directory and execute the tools there.
You can retrieve the sandbox directory for a slave node from the Cassandra-Mesos REST API or from the Mesos web UI.

## QA report collection

Cassandra-Mesos provides a tool to collect and download information from all of your Cassandra nodes into 
a single directory. `com-qa-report` downloads the log files from the executor (`executor.log`), Cassandra
(`system.log`), and the results of `nodetool version`/`info`/`status`/`tpstats` of each node into a single
directory.

This should make debugging and bug hunting a bit easier for you and us.

Note, that it `com-qa-report` requires the JMX port to be accessible from the host executing `com-qa-report`.  

# REST API features

This list highlights some administrative features that are performed by Cassandra-Mesos.

* Cluster-wide repair. Performs a `nodetool repair` *WITHOUT* 
  `--partitioner-range --full --in-local-dc --sequential` options. A maximum of one node will perform a repair at any
  time. No cleanup will "disturb" the cluster-wide repair.
* Cluster-wide cleanup. Similar to cluster-wide repair - a maximum of one node performs a cleanup - no
  repair will "disturb" the cluster-wide cleanup.
* Cluster-wide restart - this is a _rolling restart_ and does not interfer with cluster-wide repair or claanup.
* View configuration
* View Cassandra nodes and health check information
* Retrieve list of live endpoints (as JSON or plain text)
* Restart a node, stop a node, start a node
* Replace a node
* Add more nodes (scale out)
* Convert a non-seed node to a seed node, and vice-versa

# Driver extension

To use the Cassandra cluster in your application, an extension to the [DataStax Java Driver] has been
developed.

```java
// Place the base-URI of the Cassandra-Mesos REST API here.
// It usually looks like this:     http://1.2.3.4:18080/
String httpServerBaseUri = "http://1.2.3.4:18080/";
// Specify how many live nodes you want to provide to the Cluster.Builder instance.
int numberOfContactPoints = 3;
Cluster.Builder clusterBuilder =
    CassandraOnMesos.forClusterBuilder(Cluster.builder())
        .withApiEndpoint(httpServerBaseUri)
        .withNumberOfContactPoints(numberOfContactPoints)
        .build();
```

# Unsupported or unimplemented features

* Multi-datacenter configurations are not implemented yet.
* Cassandra's Rack/DC awareness is not supported yet, but we encourage you to choose a datacenter and rack name for your cluster.
* Cassandra software updates including rolling restart is not implemented yet. Minor version updates might be easy but major version updates might require additional tasks like `sstableupgrade` - so major software upgrades are a much harder task.
* Executor software updates are not implemented yet.
* SSL/encryption configuration is currently not supported.
* Support for `sstable*` tools
* Support for OpsCenter community edition
* JMX authentication

[Apache Cassandra]: http://cassandra.apache.org/
[Apache Mesos]: http://mesos.apache.org/
[Apache ZooKeeper]: http://zookeeper.apache.org/
[Marathon]: https://mesosphere.github.io/marathon/
[Mesos-DNS]: https://mesosphere.github.io/mesos-dns/
[DataStax Java Driver]: https://datastax.github.io/java-driver/