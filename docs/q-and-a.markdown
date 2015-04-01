Cassandra-on-Mesos - Q & A
==============================

------------

**DISCLAIMER**
_This is  a very early version of Cassandra-on-Mesos framework. This
document, code behavior, and anything else may change without notice and/or break older installations._

------------



* **Troubleshoting when no Cassandra process is started**

  The Cassandra-on-Mesos framework first allocates
  executors for all seed nodes. It starts the Cassandra processes after the initially configured number of seed nodes have been allocated.
  
  _Example:_ If you have configured 3 seed nodes, but only 2 executors are allocated, no Cassandra process
  is started. After the 3rd executor is allocated, all Cassandra processes are started in the normal fashion.

* **Cassandra processes have started but it takes a long time until the whole cluster is running**

  Here are the defined Cassandra-on-Mesos
  limitations before a Cassandra process can be started:
  
  1. A minimum of one Cassandra seed node must be running and in operation-mode `NORMAL.
     Otherwise, other nodes cannot join the cluster.
  1. All other nodes must be in operation-mode `NORMAL`.
  1. Only a single Cassandra processes can be started within the configuration parameter `bootstrapGraceTimeSeconds`.
     This allows a previously started node to successfully bootstrap.

* **Are multi-homed servers supported?**

  Yes, if you configure Apache Mesos and Apache Cassandra correctly.

  **Security risk notice:**
  If you have an interface that faces the public internet and you expose ports that are used by Apache Mesos and/or
  Apache Cassandra to the public internet, you - _(sorry for that expressive words)_ - should shoot yourself
  in the head. **Never expose ports to the public internet and configure your firewall rules carefully!**
  
  **Special note on JMX port** Sun/Oracle do not
  bind the JMX network listener to a specific IP and/or interface, which means that JMX is available on all
  interfaces.

* **JMX port of Apache Cassandra**

  A possible security risk in Apache has been closed in versions 2.0.14 and 2.1.4. This had impact to the
  current development of the Cassandra-on-Mesos framework. Since 2.0.14 and 2.1.4 Apache Cassandra only opens the
  JMX port (usually 7199) on the loopback address (`127.0.0.1`). If you wish to open it **and know that you
  introduce a possible security risk**, you can pass the environment variables
  `CASSANDRA_JMX_LOCAL=false` and `CASSANDRA_JMX_NO_AUTHENTICATION=true` to the framework upon **initial** invocation
  (i.e. when the framework first registers).

  [CASSANDRA-9089](https://issues.apache.org/jira/browse/CASSANDRA-9089) is meant to let JMX listen to a
  specific IP address, but is is not included in Cassandra 2.1.4.

  References:

  * [Security announcement](http://www.mail-archive.com/user@cassandra.apache.org/msg41819.html)
  * [CASSANDRA-9085](https://issues.apache.org/jira/browse/CASSANDRA-9085)
  * [JMX security](https://wiki.apache.org/cassandra/JmxSecurity)
  * [JMX agent](http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html)

* **Which versions of Apache Cassandra are supported?**

  All Apache Cassandra versions from 2.1 forward work with this version of the Cassandra-on-Mesos framework.
  **Note** Currently there is no official mechanism to upgrade from one version to another.

* **Cassandra nodetool complains with strange exceptions**

  The  `nodetool` and  `com-nodetool` are not designed to be executed against
  releases outside of the ones they are taken from. This means that a `nodetool` command from 2.1.2 issued against
  2.0.14 may not work. There is no guaranteed compatibility in
  JMX between versions of Apache Cassandra.

