---
title: Backup and Restore
---

# Backup and Restore

Cassandra-Mesos supports backup and restore operation. These operations are exposed through a REST API.

# Configuration

Configuration is provided via the environment variable `CASSANDRA_BACKUP_DIRECTORY`. This directory specifies where
to store named backups. The default directory is `backup` inside the task's sandbox.

# Example

Suppose we have started Cassandra-Mesos cluster having 2+ nodes.

Let's create keyspace `t` with table `test` having some data:
```
CREATE KEYSPACE t WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
USE t;
CREATE TABLE test (id int PRIMARY KEY, s varchar);

INSERT INTO test (id, s) VALUES (1, '1');
INSERT INTO test (id, s) VALUES (2, '2');
INSERT INTO test (id, s) VALUES (3, '3');
INSERT INTO test (id, s) VALUES (4, '4');
INSERT INTO test (id, s) VALUES (5, '5');
INSERT INTO test (id, s) VALUES (6, '6');
INSERT INTO test (id, s) VALUES (7, '7');
INSERT INTO test (id, s) VALUES (8, '8');
INSERT INTO test (id, s) VALUES (9, '9');
INSERT INTO test (id, s) VALUES (10, '10');
```

After that table `test` should contain rows with ids from 1 to 10 inclusive.

Let's do following steps:
1. start backup using `/cluster/backup/start?name=backup-0` method;
2. wait until backup is finished polling `/cluster/backup/status` method;

Backup is created in $backupDir/$clusterName/$executorId/$backupName/$keyspace/$table for each keyspace and table, where:
- $backupDir - value of CASSANDRA_BACKUP_DIRECTORY option;
- $clusterName - name of the cluster;
- $executorId - identifier of executor used to run Cassandra node;
- $backupName - name of the backup;
- $keyspace - name of the keyspace;
- $table - name of the table;

Now let's make some modifications to `t.test` data:
```
delete from test where id in (1,5,7,10);
```
Data is deleted and table `test` is missing rows with the above ids.

Let's perform a restore:
1. start restore using `/cluster/restore/start?name=backup-0` method;
2. wait until restore is finished polling `/cluster/restore/status` method;

After restore is finished data in `test` should be exactly the same as before modifications,
containing all ids from 1 to 10 inclusive.

# How it works

Backup stores copies of each table of each keyspace except "system" and "system_traces" keyspaces.
Backup procedure internally uses Cassandra snapshots mechanism.

Both procedures are executed sequentially one-by-one on every Cassandra node.

Backup is created in following way (for each keyspace):
1. Snapshot is created;
2. Snapshot is copied to $backupDir/$clusterName/$executorId/$backupName/$keyspace directory;
3. Snapshot is cleared;

Restore procedure works in the opposite way (for each keyspace):
1. Truncate is executed for each table (only once for specific table);
2. Data is restored from saved backup;
3. Data is reloaded (using loadNewSSTables JMX call);

# Limitations

## No atomicity.
Backup and Restore procedures are not atomic. If DB is actively modified during backup or restore both procedures could
produce inconsistent (from the user's perspective) data sets. So it is recommended to create a backup (and restore from
a backup) during DB inactivity time.

## Executor ID preservation
Backup and Restore uses directory structure that contains $executorId component. That means, that if Cassandra-Mesos
is restarted using new Executor, it would be not possible to restore from the old backup. Manual directory renaming should
be done to make directory structure match new $executorId.
