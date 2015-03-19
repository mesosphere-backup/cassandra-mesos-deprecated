Cassandra-on-Mesos - Q &amp; A
==============================

------------

**DISCLAIMER**
_You are looking at a very early version of Cassandra-on-Meos framework. Things mentioned in this
document, behavior implemented in the code and anything else may change without notice and/or break older installations._

------------

# Misc.

* **No Cassandra process is ever started**

  The very first thing the Cassandra-on-Mesos framework does is to allocate
  executors for all seed nodes. It will only start Cassandra processes when the initially configured number of seed
  nodes have been allocated.
  
  _Example:_ if you have configured 3 seed nodes but only 2 executors could be allocated, no Cassandra process
  will be started. When the 3rd executor can be allocated, all Cassandra processes will be started using the normal
  restrictions.

* **Cassandra processes are started but it requires a long time until the whole cluster is running**

  The Cassandra-on-Mesos
  framework currently defines the following limitations before a Cassandra process can be started:
  
  1. At least one Cassandra seed node must be running and must be in operation-mode `NORMAL.
     Otherwise other nodes would never succeed to join the cluster.
  1. All other nodes must be in operation-mode `NORMAL`.
  1. No two Cassandra processes must be started within the configuration parameter `bootstrapGraceTimeSeconds`.
     This is to allow a previously started node to successfully bootstrap.

* **Are multi homed servers supported**

  Generally yes, if you configure Apache Mesos and Apache Cassandra correctly.

  If you have an interface facing the public internet and expose ports used by Apache Mesos and/or
  Apache Cassandra to the public internet, you - _(sorry for that expressive words)_ - should shoot yourself
  in the head. **Do never expose ports to the public internet and configure your firewall rules carefully!**
  
  **Special note on JMX port** Take care on the JMX port. Sun/Oracle do not think that it may make sense to
  bind the JMX network listener to a specific IP and/or interface. This means, that JMX is available on all
  interface.

* **Which versions of Apache Cassandra are supported**

  Generally all Apache Cassandra versions since 2.1 work with this version of the Cassandra-on-Mesos framework.
  Please note that currently there is no official mechanism to upgrade from one version to another.

* **Cassandra nodetool complains with strange exceptions**

  As a general note: `nodetool` (and therefore also `com-nodetool`) are not meant to be executed against
  other releases than that they've been taken from. That means a `nodetool` command from 2.1.2 issued against
  2.0.14 may work - but do not expect it to work. There is nothing like a guaruanteed compatibility regarding
  JMX between versions of Apache Cassandra.
