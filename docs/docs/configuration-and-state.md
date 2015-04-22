---
title: Configuration and State
---

------------

**DISCLAIMER**
_This is a very early version of Cassandra-on-Mesos framework. This document, code behavior, and anything else may change without notice and/or break older installations._

------------

# Configuration and State

This document is a technical description about what happens inside Cassandra-on-Mesos framework -
more precisely: inside the scheduler.

Reminder: the single instance of the Cassandra-on-Mesos _scheduler_ submits tasks (and status requests) to  its Cassandra-on-Mesos _executors_. Configuration and status information is stored as state objects
in ZooKeeper - and the _scheduler_ is the only process that is updates that information.
Cassandra-on-Mesos _executors_ are relatively dumb processes that just do what the _scheduler_ wants them to do.

This document assumes that the reader is familiar with Apache Mesos and (to some extent) with
Apache Cassandra.

# State objects

To have a better understanding about what's going on in the _scheduler_, it is essential to know the structures defined in `model.proto`.

The framework stores its state object in ZooKeeper at `zk://<server-list>/cassandra-mesos/<framework-name>` (let's call this the _ZooKeeper base URL_).

Since the framework name is encoded in the ZK URL you can spin off multiple framework instances with different names that can run concurrently on the same cluster.

There are some objects stored directly beneath the _ZooKeeper base URL_. All of these objects are defined in
`model.proto` and are stored in ZK using their message name.

* `CassandraFrameworkConfiguration` Base configuration object including all configuration role objects.
* `CassandraClusterState` Current status of the cluster.
* `CassandraClusterHealthCheckHistory` Contains a history of the last health-checks that were received from all nodes.
* `CassandraClusterJobs` Contains the current cluster-wide job and the last job status (one per job type).

These objects are initially persisted when you start a Cassandra-on-Mesos framework for the first time.

# General procedure

Mesos periodically offers the Cassandra-on-Mesos scheduler available resources via a call to `Scheduler.resourceOffers`. Within this call, the Cassandra-on-Mesos framework checks whether nodes need to be "occupied" as a Cassandra node or tasks have to be submitted against already acquired nodes.

The framework currently only allows one Cassandra instance per node (based on IP address).

## Offer handling in the scheduler

If `CassandraClusterState.nodesToAcquire` is greater than 0 and an offer is received for a node that does not
already host a Cassandra instance, a Cassandra-on-Mesos executor is launched on that node.

If `CassandraClusterState.seedsToAcquire` is also greater than 0, the new Cassandra instance will be a seed node.

If an offer for a node already containing an Cassandra-on-Mesos executor is received, offer handling checks whether tasks need to be launched via that executor, or a health check or a status report for a cluster-wide
job is required.

Tasks are launched via `SchedulerDriver.launchTasks()`. Such tasks can be:

1. Executor metadata - to inquire some executor and slave node metadata. This tasks ends when the executor itself
   terminates.
1. Cassandra server - to start the Cassandra process.
1. Cluster job - to start a node's part of a cluster-wide job (repair and cleanup).

Messages are submitted via `SchedulerDriver.sendFrameworkMessage()`. Such messages can be:

1. Health check - to check the status of a Cassandra process
1. Cluster job status - to check the status of a node's part of a cluster-wide job (repair and cleanup)


![Offers]({{ site.baseurl }}/img/offer-and-comm.png "Offers")

![Offer handling details]({{ site.baseurl }}/img/offer-handling.png "Offer handling details")

## Node status

A `CassandraNode` is created with `targetRunState=RUN`. There are four run states defined. It is possible to set the
`targetRunState` from any state to any state - except that it is not possible to change the `targetRunState` once
it is set to `TERMINATE`.
 
* `RUN` the normal and initial state. It means that the scheduler will do its best to keep the Cassandra process
  running. It will be started if necessary.
* `STOP` means that the Cassandra process should not run. It will be stopped if necessary.
* `RESTART` is basically a automatic transition via `STOP` to `RUN`. After the Cassandra process has been stopped
  and is running again, the `targetRunState` is set to `RUN`.
* `TERMINATE` is a 'dead end state'. It means to stop the Cassandra process and the executor. It is not possible
  to set `targetRunState` for this node to another value. All you can do is to do initiate a node-replace for
  this node.

![CassandraNode targetRunState transitions]({{ site.baseurl }}/img/node-state-transition.png "CassandraNode targetRunState transitions")

## Cluster jobs

Cassandra-on-Mesos framework uses so called _cluster jobs_ to ensure that certain tasks like
_repair_, _cleanup_ and _restart_ are never executed on/against more than one node. Some job types
(_repair_ and _cleanup_) involve activity performed against the actual Cassandra process. Other job types
(_restart_) are just a kind of synchonization object.

Only one cluster job can be active at all times. A cluster job can be aborted - effectively after the current
node has finished its part. At the moment there is no way to interrupt a node's part of a cluster job.

A node's part of a cluster job is effectively cancelled when the Cassandra process or its executor or its
slave dies.

![Cluster job diagram]({{ site.baseurl }}/img/cluster-jobs.png "Cluster job diagram")


## Task states

The `CassandraNode` keeps track of all tasks that have been launched via the executor. Again, there are three kinds
of tasks that can be launched: executor metadata, cassandra server and cluster job.

Each started task is added to `CassandraNode.tasks`. A task is only removed from that list, when the scheduler receives
the appropiate status update from Mesos via `Scheduler.statusUpdate()`. This means, that the `CassandraNode.tasks`
list contains only running tasks.

There are three kinds of tasks:

* `METADATA` is used to inquire some information about the node the executor runs on.
  This task does not spawn a process.
* `SERVER` is the task that writes Cassandra configuration files and starts the Cassandra process.
  Task status changes from executor to scheduler represent whether the Cassandra process is running.
* `CLUSTER_JOB` is the task that represents the node's part of a cluster wide job like _repair_ or _cleanup_.
  This task does not spawn a process.
* `CONFIG_UPDATE` is the task that just updates Cassandra configuration files.
  This task does not spawn a process.

The lifecycle of a task is communicated via the standard Mesos messages. 

## Framework messages

The scheduler requests status information from executors periodically via so called Mesos framework messages. Framework messages are not guaranteed to arrive in order or to even be delivered at all.

Cassandra-on-Mesos uses two kinds of status request/response framework messages:

* `HEALTH_CHECK_DETAILS` to get information about the status of the Cassandra process.
* `NODE_JOB_STATUS` to get information about the status and progress of a node's part of a cluster-wide
  job like _repair_ or _cleanup_.

The executor can submit framework messages on its own as part of another task - for example when detecting
that the Cassandra process died or to inform the scheduler about a status change.

## "node-is-live" detection

Sometimes the framework has to ensure that a Cassandra node can be considered as "live". A Cassandra node
is considered "live", when all the following criteria match (in `CassandraCluster.isLiveNode()`):

1. The `CassandraNode` has a `cassandraNodeExecutor` field value.
1. The `CassandraNode` has a task of type `SERVER` (which means that the Cassandra process is running).
1. The `CassandraNode` has a recent `HealthCheckHistoryEntry`, which is `healthy` and has a `info`field value.
1. The `NodeInfo` in the recent `HealthCheckHistoryEntry` indicates that both native transport and thrift transport
   are running.

## Cassandra process startup restrictions

The Cassandra-on-Mesos framework currently defines the following limitations before a Cassandra process can be started:

1. At least one Cassandra seed node must be running and must be in operation-mode `NORMAL.
   Otherwise new nodes would never successfully join the cluster.
1. All other nodes must be in the `NORMAL` operation mode.
1. No two Cassandra processes must be started within the configuration parameter `bootstrapGraceTimeSeconds`.
   This is to allow a previously started node to successfully bootstrap.

# Initial startup

The inital state (`CassandraClusterState`) is basically empty and has the fields `nodesToAcquire` set to the
configured number of nodes and `seedsToAcquire` set to the number of seeds.

At first, the scheduler just acquires the number of seed nodes by allocating the required resources and
launching the Cassandra-on-Mesos executor. No Cassandra process will be started until there are enough seed nodes to fulfil the initially configured number of seeds (via `CassandraFrameworkConfiguration.defaultConfigRole.numberOfSeeds`).