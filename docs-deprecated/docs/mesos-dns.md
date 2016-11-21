Mesos DNS
=========

Mesos DNS can be used to provide DNS Records (A and SRV) for tasks running on a mesos cluster.  This doc will explain how the Mesos DNS integration works for the Cassandra Framework.

## An Example

In the case of Cassandra running on DCOS the (default) Cluster name is `dcos`. The framework name registered with Mesos is `cassandra.dcos`.  The task name for a running Cassandra server is `cassandra.dcos.node`,

If you were to change the cluster name to "foo", the framework name would now be `cassandra.foo` and the server task names would now be `cassandra.foo.node`.

To access your "foo" Cassandra cluster you would use `cassandra-foo-node.cassandra.foo.mesos`.


## How it works

The DNS names that are created by mesos-dns follow a specific schema, all of which can be found in the official documentation[1].

To summarize the documentation here, mesos-dns creates a DNS name with the following format: `taskName.frameworkName.mesos`.

In the case of Cassandra the task name is `cassandra.dcos.node` which mesos-dns turns into `cassandra-dcos-node` since it doesn't all `.` in task names.  The framework name `cassandra.dcos` is allowed to have `.` in it so that stays the same. And mesos is the default value for TLD.

When we put it altogether this is `cassandra-dcos-node.cassandra.dcos.mesos`.

The original intent was to have a name of `node.dcos.cassandra.mesos` but due to time constraints and a misunderstanding of how mesos-dns worked, this is what we're left with.  Hopefully it can be cleaned up in the future.

[1] http://mesosphere.github.io/mesos-dns/docs/naming.html

