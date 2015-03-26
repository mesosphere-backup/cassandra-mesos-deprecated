Cassandra-on-Mesos vs. on-premise Cassandra
===========================================

------------

**DISCLAIMER**
_You are looking at a very early version of Cassandra-on-Meos framework. Things mentioned in this
document, behavior implemented in the code and anything else may change without notice and/or break older installations._

------------


When running [Apache Cassandra] on [Apache Mesos] there are 
some things that are different. This document shall give people with experience on Apache Cassandra an overview
of the differences. Technical details are out of scope in this document - we only outline the differences here.

# Apache Mesos and Apache Cassandra

## Short Apache Mesos introduction

Apache Mesos allows you to handle distributed applications in your datacenter. Basically all you need to do
is to define the number of nodes and the resource requirements (RAM, disk, CPU) per node. Everything else is
done transparently for you.

Apache Mesos uses [Apache ZooKeeper] to persist its state. Apache ZooKeeper is usually used in a high-availability
configuration. "State" means everything beginning with configuration, via nodes (including their status and load) up
to deployed frameworks like Cassandra-on-Mesos that also store their configuration and current status.

Mesos introduces some terms that you should be familiar with:

* **Framework** some software that allows to run an application (in this case Apache Cassandra) on
  Apache Mesos.
* **Scheduler** is the _intelligent_ part in the framework. It is responsible to manage configuration,
  track state changes and also provide some API (in this case a simple REST API). There is only one
  instance of the scheduler per framework at any time - so it is highly recommended to use [Marathon]
  to keep it running. To be clear: if the scheduler is not running, the Cassandra cluster continues to run
  and it safe to stop the scheduler and restart it on another machine.
* **Executor** is the _dumb_ part in the framework running on each node. Executors are started by Mesos.
  They are called by the scheduler and are able to launch tasks (like "launch Cassandra process").

## Usual Apache Cassandra on-prem installation

Usually you pick a number of nodes you want to run Cassandra on, then install OS, JRE and Apache Cassandra on
these nodes, configure the nodes - and don't forget to choose a set of seed nodes.
LD;DR - you do everything on your own.

# Initial Cassandra-on-Mesos setup

A bullet list of what you need to run Apache Cassandra on Apache Mesos follows. If you want to try
Cassandra-on-Mesos on your laptop, just do it - there's a separate document explaining how to setup a test/dev
environment.

# Prerequisites

* [Apache Mesos]: Mesos and ZooKeeper in a HA configuration
* [Marathon]: (optional) cluster-wide init and control system for services
* [Mesos-DNS]: (optional) DNS-based service discovery for Mesos
* Bunch of servers with *local storage* for Cassandra configured as Mesos slaves - up and running
* Knowledge about how many resources each of your Cassandra nodes needs (RAM, disk space, CPU)

# Configuration

Cassandra-on-Mesos framework needs to know some things before you can get started:

* Number of Cassandra nodes
* Number of seed nodes
* Number of CPU cores per node
* Amount of RAM per node (see below)
* Amount of disks and space per disk (see below)
* Name of the cluster
* Name of Mesos role
* Additional, non-default configuration in `cassandra.yaml`
* Name of the snitch, name of the datacenter and name of the rack (see below)

Configuring RAM requires two parameter. The first one (used by Mesos for provisioning) is the total amount
of RAM. The second is the heap size of the Java VM. Off-heap space used by Cassandra and operating system block level
cache has to fit into the gap between these two parameters.

Mesos acquires a workspace (a directory) per executor. Usually that workspace is always recreated when the
executor starts - means: the executor will get an empty directory on each start. Since this is not a good
behavior when using databases (Cassandra is a database), Mesos 0.22 introduces the ability to allocate
multiple permanent disk locations (*not* ephemeral) that can be reused when the executor restarts.

Apache Mesos does not know about different data centers or racks on its own like Apache Cassandra. At the moment
Cassandra-on-Mesos does not support multiple datacenters or racks. But in order to be able to use that really
useful feature in one of the future releases of Cassandra-on-Mesos we've chosen to use the
`GossipingPropertyFileSnitch` by default and let you configure a datacenter and rack name.

TL;DR Cassandra-on-Mesos framework performs the following tasks for you:

* Rollout JRE to nodes (however you have to provide the framework a location where you provide a copy of the Oracle
  JRE due to legal/licensing restrictions)
* Rollout Apache Cassandra to nodes
* Configure Apache Cassandra (`cassandra.yaml` + `cassandra-rackdc.properties`)

# CLI tools

## Cassandra tool extensions

Apache Cassandra ships with the useful tools `cqlsh`, `nodetool`, `cassandra-stress`. These tools usually require you
to pass in nodes you want to connect to. This becomes a bit complicated with Mesos, since you may have no clue
even about the IPs of the seed nodes.

Cassandra-on-Mesos provides two things that solves that problem:

1. REST API endpoints to inquire (one, more or all) live Cassandra nodes
1. Shell scripts using that API

Basically you need:
 
1. an unpacked copy of Apache Cassandra
1. the set of Cassandra-on-Mesos shell scripts
1. the IP/hostname of the host running the scheduler (Mesos-DNS makes this task easier)

So, in order to run the tools mentioned above, you do:

* `cqlsh` - use `com-cqlsh` instead. You can pass all `cqlsh` options to `com-cqlsh` (except host and port)
* `nodetool` - use `com-nodetool` instead. You can pass all `nodetool` options to `com-nodetool` (except host and port)
* `cassandra-stress` - use `com-stress` instead. You can pass all `cassandra-stress` options to `com-stress` (except host and port)

The REST API endpoint chooses the node(s) from the set of live nodes randomly. If you have to use `nodetool`
against a specific node, you will have to find out that node's IP address and use `nodetool` as you are used to.

## Other Cassandra tools

Running Cassandra tools like the bunch of `sstable*` tools in `bin` and `tools/bin` is currently a bit complicated.
You have to logon to the slave, `cd` to the sandbox directory and execute the tools there.
Apologies for that. You can retrieve the sandbox directory from the REST API or from the Mesos web UI.

## QA report collection

Cassandra-on-Mesos also provides a tool to collect and download information from all your Cassandra nodes into 
one single directory. `com-qa-report` downloads the log files from the executor (`executor.log`) and Cassandra
(`system.log`) and also the results of `nodetool version`/`info`/`status`/`tpstats` of each node into a single
directory.

This should make debugging and bug hunting a bit easier for you and us.

# REST API features

The following bullet-list highlights some administrative features that are performed by Cassandra-on-Mesos
for you.

* Cluster-wide repair. Performs a `nodetool repair` *WITHOUT* 
  `--partitioner-range --full --in-local-dc --sequential` options. At most one node will perform a repair at any
  time. No cleanup will "distrub" the cluster-wide repair.
* Cluster-wide cleanup. Similar to cluster-wide repair - only one node at once performs a cleanup - no
  repair will "disturb" the cluster-wide cleanup.
* View configuration
* View Cassandra nodes and health check information
* Retrieve list of live endpoints (as JSON or plain text)
* Restart a node, stop a node, start a node
* Replace a node
* Add more nodes (scale out)
* Make non-seed node a seed node and vice-versa

# Driver extension

In order to use the Cassandra cluster in your application, an extension to the [DataStax Java Driver] has been
developed.

```
// Place the base-URI of the Cassandra-on-Mesos REST API here.
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

# Cloud computing nodes

If you run Mesos on Amazon EC2 or Google GCE, make sure that all Cassandra nodes live in one availability zone for now.

# Unsupported features, things not implemented yet

* Multi-datacenter configurations are not implemented yet.
* That also means that Cassandra's Rack/DC awareness is not supported yet, but we encourage you to choose
  a datacenter and rack name for your cluster.
* Automatic JRE download might not be ever supported by Cassandra-on-Mesos due to legal/licensing restrictions. 
* Support Mesos-DNS is not implemented yet.
* Providing descent configuration via a predefined configuration file for the initial setup is not present yet.
* Cassandra software download from official Apache mirrors will be added later.
* Cassandra software updates including rolling restart is not implemented yet. Minor version updates might be easy
  but major version updates may require additional tasks like `sstableupgrade` - so major software upgrades are
  a much harder task.
* Executor software updates are not implemented yet.
* Changes to configuration: Cassandra-on-Mesos will have different, named configurations in the future
  so that you are able to run nodes with different configurations. 
* Rolling restart is a thing that will be implemented later.
* SSL/encryption configuration is currently not supported.
* Support for `sstable*` tools
* Support for OpsCenter community edition




[Apache Cassandra]: http://cassandra.apache.org/
[Apache Mesos]: http://mesos.apache.org/
[Apache ZooKeeper]: http://zookeeper.apache.org/
[Marathon]: https://mesosphere.github.io/marathon/
[Mesos-DNS]: https://mesosphere.github.io/mesos-dns/
[DataStax Java Driver]: https://datastax.github.io/java-driver/
