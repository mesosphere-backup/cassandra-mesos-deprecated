Cassandra-on-Mesos REST API
===========================

------------

**DISCLAIMER**
_This is a very early version of Cassandra-on-Mesos framework. This
document, code behavior, and anything else may change without notice and/or break older installations._

------------

Cassandra-on-Mesos provides a REST API though its scheduler. point your browser to `http://127.0.0.1:18080/` (or wherever your 
Cassandra-on-Mesos scheduler is running).

The `/` endpoint returns a simple JSON object that lists all URLs the method to use, and the available `Content-Type`.

```json
[
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/config"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/cleanup/start"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/cleanup/abort"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/cleanup/status"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/cleanup/last"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/repair/start"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/repair/abort"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/repair/status"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/repair/last"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/rolling-restart/start"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/cluster/rolling-restart/abort"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/rolling-restart/status"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/cluster/rolling-restart/last"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/node/all"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/node/seed/all"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/stop/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/start/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/restart/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/terminate/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/replace/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/make-seed/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "POST",
        "url": "http://localhost:18080/node/{node}/make-non-seed/"
    },
    {
        "contentType": [
            "application/json"
        ],
        "method": "GET",
        "url": "http://localhost:18080/live-nodes"
    },
    {
        "contentType": [
            "text/plain"
        ],
        "method": "GET",
        "url": "http://localhost:18080/live-nodes/text"
    },
    {
        "contentType": [
            "text/x-cassandra-cqlsh"
        ],
        "method": "GET",
        "url": "http://localhost:18080/live-nodes/cqlsh"
    },
    {
        "contentType": [
            "text/x-cassandra-nodetool"
        ],
        "method": "GET",
        "url": "http://localhost:18080/live-nodes/nodetool"
    },
    {
        "contentType": [
            "text/x-cassandra-stress"
        ],
        "method": "GET",
        "url": "http://localhost:18080/live-nodes/stress"
    },
    {
        "contentType": [
            "application/json",
            "text/plain"
        ],
        "method": "GET",
        "url": "http://localhost:18080/qa/report/resources"
    }
]
```

# Summary of API endpoints

Endpoint | HTTP method | Content-Types| Description
--- | --- | --- | ---
`/config`                           | `GET`  | `application/json` | Returns the configuration.
`/cluster/cleanup/start`            | `POST` | `application/json` | Endpoints to start a cluster-wide cleanup.
`/cluster/cleanup/abort`            | `POST` | `application/json` | Abort the cluster-wide cleanup
`/cluster/cleanup/status`           | `GET`  | `application/json` | Inquire the current status.
`/cluster/cleanup/last`             | `GET`  | `application/json` | Inquire the status of the last cleanup.
`/cluster/repair/start`             | `POST` | `application/json` | Endpoints to start a cluster-wide repair.
`/cluster/repair/abort`             | `POST` | `application/json` | Abort the cluster-wide repair
`/cluster/repair/status`            | `GET`  | `application/json` | Inquire the current status.
`/cluster/repair/last`              | `GET`  | `application/json` | Inquire the status of the last repair.
`/cluster/rolling-restart/start`    | `POST` | `application/json` | Endpoints to start a cluster-wide rolling-restart.
`/cluster/rolling-restart/abort`    | `POST` | `application/json` | Abort the cluster-wide rolling-restart
`/cluster/rolling-restart/status`   | `GET`  | `application/json` | Inquire the current status.
`/cluster/rolling-restart/last`     | `GET`  | `application/json` | Inquire the status of the last rolling-restart.
`/node/all`                         | `GET`  | `application/json` | List all nodes and their status.
`/node/seed/all`                    | `GET`  | `application/json` | IP addresses of all seed nodes and native, thrift, and JMX port numbers.
`/node/{node}/stop`                 | `POST` | `application/json` | Sets the run-status of the `node` (either IP, hostname, or executor ID) to `STOP`, which ensures that the Cassandra process is not running.
`/node/{node}/start`                | `POST` | `application/json` | Sets the run-status of the `node` (either IP, hostname, or executor ID) to `RUN`, which ensures that the Cassandra process is running.
`/node/{node}/restart`              | `POST` | `application/json` | Sets the run-status of the `node` (either IP, hostname, or executor ID) to `RESTART`, which is effectively a sequence of `STOP` followed by `RUN`.
`/node/{node}/terminate`            | `POST` | `application/json` | Sets the requested run-status of the `node` (either IP, hostname, or executor ID) to `TERMINATE`, which ensures that the Cassandra node can be replaced. There's no way to bring a `terminated` node back.
`/node/{node}/replace`              | `POST` | `application/json` | Allocates a new Cassandra node that is configured to replace the given _node_ (either IP, hostname, or executor ID).
`/node/{node}/make-seed`            | `POST` | `application/json` | Converts a non-seed node to a seed node. Implicitly forces a rollout of the Cassandra configuration to all nodes.
`/node/{node}/make-non-seed`        | `POST` | `application/json` | Converts a seed node to a non-seed node. Implicitly forces a rollout of the Cassandra configuration to all nodes.
`/live-nodes`                       | `GET`  | `application/json` | Retrieve multiple live nodes, limited to 3 nodes by default. The limit can be changed with the query parameter `limit`.
`/live-nodes/text`                  | `GET`  | `text/plain`       | Similar to `/live-nodes` endpoint but it returns plain text.
`/live-nodes/cqlsh`                 | `GET`  | `text/x-cassandra-cqlsh` | Special live-nodes endpoints that produce command line options for the Cassandra tool cqlsh.
`/live-nodes/nodetool`              | `GET`  | `text/x-cassandra-nodetool` | Special live-nodes endpoints that produce command line options for the Cassandra tool nodetool.
`/live-nodes/stress`                | `GET`  | `text/x-cassandra-stress` | Special live-nodes endpoints that produce command line options for the Cassandra tool stress.
`/qa/report/resources`              | `GET`  | `application/json`, `text/plain` | Retrieve a JSON response with relevant information to create a QA report.

# Example response

## `/node/seed/all`

```json
{
    "nativePort" : 9042,
    "rpcPort" : 9160,
    "jmxPort" : 7199,
    "seeds" : [ "127.0.0.1" ]
}
```

## `/config`

```json
{
    "frameworkName" : "cassandra",
    "frameworkId" : "20150318-143436-16777343-5050-5621-0000",
    "defaultConfigRole" : {
        "cassandraVersion" : "2.1.2",
        "targetNodeCount" : 2,
        "seedNodeCount" : 1,
        "diskMb" : 2048,
        "cpuCores" : 2.0,
        "memJavaHeapMb" : 1024,
        "memAssumeOffHeapMb" : 1024,
        "memMb" : 2048,
        "taskEnv" : null
    },
    "nativePort" : 9042,
    "rpcPort" : 9160,
    "storagePort" : 7000,
    "sslStoragePort" : 7001,
    "seeds" : [ "127.0.0.1" ],
    "healthCheckIntervalSeconds" : 10,
    "bootstrapGraceTimeSeconds" : 0,
    "currentClusterTask" : null,
    "lastRepair" : null,
    "lastCleanup" : null,
    "nextPossibleServerLaunchTimestamp" : 1426685858805
}
```

## `/node/all`

```json
{
    "replaceNodes" : [ ],
    "nodesToAcquire" : 0,
    "nodes" : [ {
        "tasks" : {
            "METADATA" : {
                "cpuCores" : 0.1,
                "diskMb" : 16,
                "memMb" : 16,
                "taskId" : "cassandra.node.0.executor"
            },
            "SERVER" : {
                "cpuCores" : 2.0,
                "diskMb" : 2048,
                "memMb" : 2048,
                "taskId" : "cassandra.node.0.executor.server"
            }
        },
        "executorId" : "cassandra.node.0.executor",
        "ip" : "127.0.0.2",
        "hostname" : "127.0.0.2",
        "targetRunState" : "RUN",
        "jmxPort" : 64112,
        "seedNode" : true,
        "cassandraDaemonPid" : 6104,
        "lastHealthCheck" : 1426686217128,
        "healthCheckDetails" : {
            "healthy" : true,
            "msg" : "",
            "version" : "2.1.2",
            "operationMode" : "NORMAL",
            "clusterName" : "cassandra",
            "dataCenter" : "DC1",
            "rack" : "RAC1",
            "endpoint" : "127.0.0.2",
            "hostId" : "4207396e-6aa0-432e-97d9-1a4df3c1057f",
            "joined" : true,
            "gossipInitialized" : true,
            "gossipRunning" : true,
            "nativeTransportRunning" : true,
            "rpcServerRunning" : true,
            "tokenCount" : 256,
            "uptimeMillis" : 29072
        }
    }, {
        "tasks" : {
        "METADATA" : {
            "cpuCores" : 0.1,
            "diskMb" : 16,
            "memMb" : 16,
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor"
        },
        "SERVER" : {
            "cpuCores" : 2.0,
            "diskMb" : 2048,
            "memMb" : 2048,
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.server"
        }
        },
        "executorId" : "cassandra.node.1.executor",
        "ip" : "127.0.0.1",
        "hostname" : "localhost",
        "targetRunState" : "RUN",
        "jmxPort" : 64113,
        "seedNode" : false,
        "cassandraDaemonPid" : 6127,
        "lastHealthCheck" : 1426686217095,
        "healthCheckDetails" : {
            "healthy" : true,
            "msg" : "",
            "version" : "2.1.2",
            "operationMode" : "JOINING",
            "clusterName" : "cassandra",
            "dataCenter" : "",
            "rack" : "",
            "endpoint" : "",
            "hostId" : "",
            "joined" : true,
            "gossipInitialized" : true,
            "gossipRunning" : true,
            "nativeTransportRunning" : false,
            "rpcServerRunning" : false,
            "tokenCount" : 0,
            "uptimeMillis" : 16936
        }
    } ]
}
```

## `/live-nodes?limit=N`

```json
{
    "nativePort" : 9042,
    "rpcPort" : 9160,
    "jmxPort" : 7199,
    "liveNodes" : [ "127.0.0.1", "127.0.0.2" ]
}
```

## `/live-nodes/text?limit=N`

```json
NATIVE: 9042
RPC: 9160
JMX: 7199
IP: 127.0.0.1
IP: 127.0.0.2
```

## `/cluster/repair/start`

```json
{
     "started" : true
}
```

## `/cluster/repair/status`

```json
{
    "running" : true,
    "repair" : {
        "type" : "REPAIR",
        "started" : 1426686829672,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ ],
        "currentNode" : {
            "executorId" : "cassandra.node.0.executor",
            "taskId" : "cassandra.node.0.executor.REPAIR",
            "hostname" : "127.0.0.2",
            "ip" : "127.0.0.2",
            "processedKeyspaces" : { },
            "remainingKeyspaces" : [ ]
        },
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.REPAIR",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : {
                "system_traces" : {
                "status" : "FINISHED",
                "durationMillis" : 2490
                }
            },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/cluster/repair/abort`

```json
{
    "aborted" : true
}
```

## `/cluster/repair/last`

```json
{
    "present" : true,
    "repair" : {
        "type" : "REPAIR",
        "started" : 1426686829672,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ ],
        "currentNode" : {
            "executorId" : "cassandra.node.0.executor",
            "taskId" : "cassandra.node.0.executor.REPAIR",
            "hostname" : "127.0.0.2",
            "ip" : "127.0.0.2",
            "processedKeyspaces" : { },
            "remainingKeyspaces" : [ ]
        },
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.REPAIR",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : {
                "system_traces" : {
                "status" : "FINISHED",
                "durationMillis" : 2490
                }
            },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/cluster/cleanup/start`

```json
{
     "started" : true
}
```

## `/cluster/cleanup/status`

```json
{
    "running" : true,
    "cleanup" : {
        "type" : "CLEANUP",
        "started" : 1426687019998,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ "cassandra.node.0.executor" ],
        "currentNode" : null,
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.CLEANUP",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : {
                "system_traces" : {
                "status" : "SUCCESS",
                "durationMillis" : 20
            }
            },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/cluster/cleanup/abort`

```json
{
    "aborted" : true
}
```

## `/cluster/cleanup/last`

```json
{
    "present" : true,
    "cleanup" : {
        "type" : "CLEANUP",
        "started" : 1426687019998,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ "cassandra.node.0.executor" ],
        "currentNode" : null,
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.CLEANUP",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : {
                "system_traces" : {
                "status" : "SUCCESS",
                "durationMillis" : 20
            }
            },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/cluster/rolling-restart/start`

```json
{
     "started" : true
}
```

## `/cluster/rolling-restart/status`

```json
{
    "restart" : true,
    "cleanup" : {
        "type" : "RESTART",
        "started" : 1426687019998,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ "cassandra.node.0.executor" ],
        "currentNode" : null,
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.RESTART",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : { },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/cluster/rolling-restart/abort`

```json
{
    "aborted" : true
}
```

## `/cluster/rolling-restart/last`

```json
{
    "present" : true,
    "cleanup" : {
        "type" : "RESTART",
        "started" : 1426687019998,
        "finished" : null,
        "aborted" : false,
        "remainingNodes" : [ "cassandra.node.0.executor" ],
        "currentNode" : null,
        "completedNodes" : [ {
            "executorId" : "cassandra.node.1.executor",
            "taskId" : "cassandra.node.1.executor.RESTART",
            "hostname" : "localhost",
            "ip" : "127.0.0.1",
            "processedKeyspaces" : { },
            "remainingKeyspaces" : [ ]
        } ]
    }
}
```

## `/node/{node}/stop`

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "STOP"
}
```

## `/node/{node}/run`

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RUN"
}
```

## `/node/{node}/restart`

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RESTART"
}
```

## `/node/{node}/terminate`

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "TERMINATE"
}
```

## `/node/{node}/replace`

```json
{
   "success" : "false",
   "reason" : "Some error message"
}
```


```json
{
   "success" : "true",
   "ipToReplace" : "127.0.0.1",
   "hostname" : "localhost",
   "targetRunState": "TERMINATE"
}
```

## `/node/{node}/make-seed`

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "oldSeedState" : "false",
   "success" : "false",
   "error" : "Some error message"
}
```

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "oldSeedState" : "false",
   "success" : "true",
   "seedState" : "true"
}
```

## `/node/{node}/make-non-seed`

Similar to `/node/{node}/non-seed`.

## `/qa/report/resources`

```json
{
  "nodes" : {
    "cassandra.node.0.executor" : {
      "workdir" : "/private/tmp/mesos/slave2/slaves/20150402-133617-16777343-5050-33867-S2/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.0.executor/runs/c669173c-f122-4f98-aa30-62836c128597",
      "slaveBaseUri" : "http://127.0.0.2:5051/",
      "ip" : "127.0.0.2",
      "hostname" : "127.0.0.2",
      "targetRunState" : "RUN",
      "jmxIp" : "127.0.0.1",
      "jmxPort" : 62008,
      "live" : false,
      "logfiles" : [ "/private/tmp/mesos/slave2/slaves/20150402-133617-16777343-5050-33867-S2/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.0.executor/runs/c669173c-f122-4f98-aa30-62836c128597/executor.log", "/private/tmp/mesos/slave2/slaves/20150402-133617-16777343-5050-33867-S2/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.0.executor/runs/c669173c-f122-4f98-aa30-62836c128597/apache-cassandra-2.1.4/logs/system.log" ]
    },
    "cassandra.node.1.executor" : {
      "workdir" : "/private/tmp/mesos/slave1/slaves/20150402-133617-16777343-5050-33867-S0/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.1.executor/runs/40a91e16-fde8-4b29-b4f7-6dc01c9206ad",
      "slaveBaseUri" : "http://127.0.0.1:5051/",
      "ip" : "127.0.0.1",
      "hostname" : "localhost",
      "targetRunState" : "RUN",
      "jmxIp" : "127.0.0.1",
      "jmxPort" : 62009,
      "live" : false,
      "logfiles" : [ "/private/tmp/mesos/slave1/slaves/20150402-133617-16777343-5050-33867-S0/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.1.executor/runs/40a91e16-fde8-4b29-b4f7-6dc01c9206ad/executor.log", "/private/tmp/mesos/slave1/slaves/20150402-133617-16777343-5050-33867-S0/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.1.executor/runs/40a91e16-fde8-4b29-b4f7-6dc01c9206ad/apache-cassandra-2.1.4/logs/system.log" ]
    }
  }
}
```

## GET `/qa/report/resources` Accept: text/plain

```
JMX_PORT: 62008
JMX_IP: 127.0.0.1
NODE_IP: 127.0.0.2
BASE: http://127.0.0.2:5051/
LOG: /private/tmp/mesos/slave2/slaves/20150402-133617-16777343-5050-33867-S2/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.0.executor/runs/c669173c-f122-4f98-aa30-62836c128597/executor.log
LOG: /private/tmp/mesos/slave2/slaves/20150402-133617-16777343-5050-33867-S2/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.0.executor/runs/c669173c-f122-4f98-aa30-62836c128597/apache-cassandra-2.1.4/logs/system.log
JMX_PORT: 62009
JMX_IP: 127.0.0.1
NODE_IP: 127.0.0.1
BASE: http://127.0.0.1:5051/
LOG: /private/tmp/mesos/slave1/slaves/20150402-133617-16777343-5050-33867-S0/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.1.executor/runs/40a91e16-fde8-4b29-b4f7-6dc01c9206ad/executor.log
LOG: /private/tmp/mesos/slave1/slaves/20150402-133617-16777343-5050-33867-S0/frameworks/20150402-133617-16777343-5050-33867-0001/executors/cassandra.node.1.executor/runs/40a91e16-fde8-4b29-b4f7-6dc01c9206ad/apache-cassandra-2.1.4/logs/system.log
```

