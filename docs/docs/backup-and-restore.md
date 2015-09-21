---
title: Backup and Restore
---

# Backup and Restore

Cassandra-Mesos supports backup and restore operations. These operations are exposed through a REST API.

## Configuration

You can configure the directory where backups are stored via the environment variable `CASSANDRA_BACKUP_DIRECTORY`. The default directory is `backup` inside the task's sandbox.

## Usage

***Warning:*** If your Cassandra database is active when you perform a backup or restore, there is a high risk of inconsistent data. See the "Limitations" section below for details.

### Backup

Start a backup using the `/cluster/backup/start?name=BackupName` method, where `BackupName` is the desired name of your backup. You can determine the status of the backup by polling the `/cluster/backup/status` method. Refer to [the REST API documentation](http://mesosphere.github.io/cassandra-mesos/docs/rest-api.html) for details.

A backup is created in $backupDir/$clusterName/$executorId/$backupName/$keyspace/$table for each keyspace and table, where:
- $backupDir - The value of CASSANDRA_BACKUP_DIRECTORY environment variable
- $clusterName - The name of the cluster.
- $executorId - The identifier of the executor that is used to run the Cassandra node.
- $backupName - The name of the backup.
- $keyspace - The name of the keyspace.
- $table - The name of the table.

### Restore

Start a restore using the `/cluster/restore/start?name=BackupName` method, where `BackupName` is the name of the backup that you wish to restore. You can determine the status of your restore by polling the `/cluster/restore/status` method. Refer to [the REST API documentation](http://mesosphere.github.io/cassandra-mesos/docs/rest-api.html) for details.


## How It Works

The backup procedure uses Cassandra's snapshot mechanism and stores copies of each table of each keyspace except "system" and "system_traces" keyspaces.

Backup and restore are executed on every Cassandra node sequentially, one at a time.

Backups are created in the following way for each keyspace:
1. Snapshot is created.
2. Snapshot is copied to $backupDir/$clusterName/$executorId/$backupName/$keyspace directory.
3. Snapshot is cleared.

The restore procedure works in the opposite way. For each keyspace:
1. Truncate is executed once for each table.
2. Data is restored from the specified saved backup.
3. Data is reloaded using the loadNewSSTables JMX call.

## Handling errors

To troubleshoot a failed backup or restore, check the `/cluster/backup/last` and `/cluster/restore/last` methods.

Below is an example of a failure returned by `/cluster/restore/last`:
```
{
  "present": true,
  "restore": {
    "type": "RESTORE",
    "started": 1442225436440,
    "finished": 1442225449012,
    "aborted": false,
    "remainingNodes": [],
    "currentNode": null,
    "completedNodes": [
      {
        "executorId": "cassandra.dev-cluster.node.0.executor",
        "taskId": "cassandra.dev-cluster.node.0.executor.RESTORE",
        "hostname": "master",
        "ip": "192.168.3.5",
        "startedTimestamp": 1442225726918,
        "finishedTimestamp": 1442225727082,
        "processedKeyspaces": {
          "t": {
            "status": "SUCCESS",
            "durationMillis": 152
          }
        },
        "remainingKeyspaces": []
      },
      {
        "executorId": "cassandra.dev-cluster.node.1.executor",
        "taskId": "cassandra.dev-cluster.node.1.executor.RESTORE",
        "hostname": "slave0",
        "ip": "192.168.3.6",
        "startedTimestamp": 1442225443392,
        "finishedTimestamp": 1442225443410,
        "processedKeyspaces": {
          "t": {
            "status": "FAILURE",
            "durationMillis": 13
          }
        },
        "remainingKeyspaces": []
      }
    ],
    "backupName": "backup"
  }
}
```
In this example, the restore operation failed on the `t.test` table on the second node. The cause of the problem can be examined in the executor's log. In the event of an error, fix the problem and re-run the backup or restore.

It is strongly recommended that you verify whether your backup and restore procedures completed successfully after their execution.

## Limitations

### No atomicity.
Backup and restore procedures are not atomic. If the database is modified during backup or restore, the resulting backed up or restored data may contain inconsistent data sets. For example, if several rows in a table are being updated while the table is being backed up, the backup may contain some updated rows and some non-updated rows.
 
 ***It is strongly recommended that you back up or restore your database only when there are no modifications being made to the data.***

### Restoring Using a Different Executor ID
Backup and restore use a directory structure that contains the $executorId component. If Cassandra-Mesos is restarted using a new executor, manually rename the backup directory to match the new $executorId.

***Warning: When a task is restarted with a new executor, the sandbox from the previous executor may be removed without warning. Exercise caution with the storage of your backups.***
