Cassandra-on-Mesos REST API
===========================

------------

**DISCLAIMER**
_This is a very early version of Cassandra-on-Mesos framework. This
document, code behavior, and anything else may change without notice and/or break older installations._

------------

Cassandra-on-Mesos provides a REST API though its scheduler. By default the REST API HTTP listener runs on port
`18080`. To get a quick introduction, point your browser to `http://127.0.0.1:18080/` (or wherever your 
Cassandra-on-Mesos scheduler is running).

The 'default' endpoint returns a simple JSON that you can use as a point-of-entry:

```json
{
  "configuration" : "http://192.168.5.101:18080/config",
  "seedNodes" : "http://192.168.5.101:18080/seed-nodes",
  "allNodes" : "http://192.168.5.101:18080/nodes",
  "repair" : {
    "start" : "http://192.168.5.101:18080/repair/start",
    "status" : "http://192.168.5.101:18080/repair/status",
    "lastStatus" : "http://192.168.5.101:18080/repair/last",
    "abort" : "http://192.168.5.101:18080/repair/abort"
  },
  "cleanup" : {
    "start" : "http://192.168.5.101:18080/cleanup/start",
    "status" : "http://192.168.5.101:18080/cleanup/status",
    "lastStatus" : "http://192.168.5.101:18080/cleanup/last",
    "abort" : "http://192.168.5.101:18080/cleanup/abort"
  }
}
```

# Summary of API endpoints

Endpoint | HTTP method | Description
--- | --- | ---
`/seed-nodes` | `GET` | IP addresses of all seed nodes and native, thrift, and JMX port numbers.
`/config` | `GET` | Returns the configuration.
`/nodes` | `GET` | List all nodes and their status.
`/live-nodes` | `GET` | Retrieve multiple live nodes, limited to 3 nodes by default. The limit can be changed with the query parameter `limit`.
`/live-nodes/text` | `GET` | Similar to `/live-nodes` endpoint but it returns plain text.
`/live-nodes/cqlsh`, `/live-nodes/nodetool`, `/live-nodes/stress` | `GET` | Special live-nodes endpoints that produce command line options for the corresponding Cassandra tools.
`/scale/nodes` | `POST` | Scale out the Cassandra cluster by increasing the number of nodes. It requires the query parameter `nodes` to define the total number of nodes.
`/repair/start`, `/repair/status`, `/repair/abort`, `/repair/last` | `POST` for `start` and `abort`, `GET` for `status` and `last`  | Endpoints to start a cluster-wide repair, inquire the current status, abort the cluster-wide repair, and inquire the status of the last repair.
`/cleanup/start`, `/cleanup/status`, `/cleanup/abort`, `/cleanup/last` | `POST` for `start` and `abort`, `GET` for `status` and `last`  | Similar to repair but for cluster-wide cleanup.
`/cluster/restart/start`, `/cluster/restart/status`, `/cluster/restart/abort`, `/cluster/restart/last` | `POST` for `start` and `abort`, `GET` for `status` and `last` | Performs a cluster-wide restart of all Cassandra server processes.
`/node/stop/`+_node_ | `POST` | Sets the run-status of the _node_ (either IP, hostname, or executor ID) to _STOP_, which ensures that the Cassandra process is not running.
`/node/run/`+_node_ | `POST` | Sets the run-status of the _node_ (either IP, hostname, or executor ID) to _RUN_, which ensures that the Cassandra process is running.
`/node/restart/`+_node_ | `POST` | Sets the run-status of the _node_ (either IP, hostname, or executor ID) to _RESTART_, which is effectively a sequence of _STOP_ followed by _RUN_.
`/node/terminate/`+_node_ | `POST` | Sets the requested run-status of the _node_ (either IP, hostname, or executor ID) to _TERMINATE_, which ensures that the Cassandra node can be replaced. There's no way to bring a _terminated_ node back.
`/node/replace/`+_node_ | `POST` | Alocates a new Cassandra node that is configured to replace the given _node_ (either IP, hostname, or executor ID).
`/node/seed/`+_node_ | `POST` | Converts a non-seed node to a seed node. Implicitly forces a rollout of the Cassandra configuration to all nodes.
`/node/non-seed/`+_node_ | `POST` | Converts a seed node to a non-seed node. Implicitly forces a rollout of the Cassandra configuration to all nodes.
`/qaReportResources` | `GET` | Retrieve a JSON response with relevant information to create a QA report.
`/qaReportResources/text` | `GET` | Retrieve a plain text response with relevant information to create a QA report.

# Example response

## `/seed-nodes`

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

## `/nodes`

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

## `/repair/start`

```json
{
     "started" : true
}
```

## `/repair/status`

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

## `/repair/abort`

```json
{
    "aborted" : true
}
```

## `/repair/last`

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

## `/cleanup/start`

```json
{
     "started" : true
}
```

## `/cleanup/status`

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

## `/cleanup/abort`

```json
{
    "aborted" : true
}
```

## `/cleanup/last`

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

## `/cluster/restart/start`

```json
{
     "started" : true
}
```

## `/cluster/restart/status`

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

## `/cluster/restart/abort`

```json
{
    "aborted" : true
}
```

## `/cluster/restart/last`

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

## `/node/stop/`+_node_

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "STOP"
}
```

## `/node/run/`+_node_

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RUN"
}
```

## `/node/restart/`+_node_

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RESTART"
}
```

## `/node/terminate/`+_node_

```json
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "TERMINATE"
}
```

## `/node/replace/`+_node_

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

## `/scale/nodes?nodes=N`

```json
{
   "oldNodeCount" : "3",
   "seedNodeCount" : "2",
   "applied" : true,
   "newNodeCount" : 5
}
```

## `/node/seed/`+_node_

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

## `/node/non-seed/`+_node_

Similar to `/node/non-seed/`+_node_ - see above.

## `/qaReportResources`

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

## `/qaReportResources/text`

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

