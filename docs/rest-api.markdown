Cassandra-on-Mesos REST API
===========================

------------

**DISCLAIMER**
_You are looking at a very early version of Cassandra-on-Meos framework. Things mentioned in this
document, behavior implemented in the code and anything else may change without notice and/or break older installations._

------------

Cassandra-on-Mesos provides a REST API though its scheduler. By default the REST API HTTP listener runs on port
18080. To get a quick introduction, point your browser to `http://127.0.0.1:18080/` (or wherever your 
Cassandra-on-Mesos scheduler is running).

The 'default' endpoint returns a simple JSON that you may use as a point-of-entry:

```
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

* `/seed-nodes` IP addresses of all seed nodes and native, thrift and JMX port numbers
* `/config` Returns the configuration.
* `/nodes` list of all nodes including their status
* `/live-nodes` retrieve multiple live nodes limited to 3 nodes by default. The limit can be changed with the
  query parameter `limit` 
* `/live-nodes/text` similar to `/live-nodes` endpoint except that it returns plain text
* `/live-nodes/cqlsh`, `/live-nodes/nodetool`, `/live-nodes/stress` special live-nodes endpoints producing
  command line options for the corresponding Cassandra tools
* `/scale/nodes` Allows to scale out the Cassandra cluster by increasing the number of nodes.
  Requires the query parameter `nodes` defining the desired number of total nodes.
  Must be submitted using HTTP method `POST`.
* `/repair/start`, `/repair/status`, `/repair/abort`, `/repair/last` Endpoints to start a cluster-wide repair,
  inquire the current status, abort the cluster-wide repair and inquire the status of the last repair.
* `/cleanup/start`, `/cleanup/status`, `/cleanup/abort`, `/cleanup/last` Similar to repair but for cluster-wide
  cleanup
* `/node/stop/`+_node_ : sets the requested run-status of the _node_ (either IP, hostname or executor ID) to
  _STOP_ ensuring that the Cassandra process is not running
* `/node/run/`+_node_ : sets the requested run-status of the _node_ (either IP, hostname or executor ID) to
  _RUN_ ensuring that the Cassandra process is running
* `/node/restart/`+_node_ : sets the requested run-status of the _node_ (either IP, hostname or executor ID) to
  _RESTART_ which is effectively a sequence of _STOP_ followed by _RUN_
* `/node/terminate/`+_node_ : sets the requested run-status of the _node_ (either IP, hostname or executor ID) to
  _TERMINATE_ ensuring that the Cassandra node can be replaced. There's no way to bring a _terminated_ node
  back.
* `/node/replace/`+_node_ : allocates a new Cassandra nodes that will be configured to replace the given
  _node_ (either IP, hostname or executor ID)
* `/scale/nodes` Allows to scale out the Cassandra cluster by increasing the number of nodes.
  Requires the query parameter `nodes` defining the desired number of total nodes.
  Must be submitted using HTTP method `POST`.
* `/node/seed/`+_node_ : Makes a non-seed node a seed node. Implicitly forces a rollout of Cassandra configuration
  to all nodes.
  Must be submitted using HTTP method `POST`.
* `/node/non-seed/`+_node_ : Makes a seed node a non-seed node. Implicitly forces a rollout of Cassandra configuration
  to all nodes.
  Must be submitted using HTTP method `POST`.

# Example response

## `/seed-nodes`

```
{
    "nativePort" : 9042,
    "rpcPort" : 9160,
    "jmxPort" : 7199,
    "seeds" : [ "127.0.0.1" ]
}
```

## `/config`

```
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

```
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

```
{
    "nativePort" : 9042,
    "rpcPort" : 9160,
    "jmxPort" : 7199,
    "liveNodes" : [ "127.0.0.1", "127.0.0.2" ]
}
```

## `/live-nodes/text?limit=N`

```
NATIVE: 9042
RPC: 9160
JMX: 7199
IP: 127.0.0.1
IP: 127.0.0.2
```

## `/repair/start`

```
{
     "started" : true
}
```

## `/repair/status`

```
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

```
{
    "aborted" : true
}
```

## `/repair/last`

```
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

```
{
     "started" : true
}
```

## `/cleanup/status`

```
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

```
{
    "aborted" : true
}
```

## `/cleanup/last`

```
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

## `/node/stop/`+_node_

```
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "STOP"
}
```

## `/node/run/`+_node_

```
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RUN"
}
```

## `/node/restart/`+_node_

```
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "RESTART"
}
```

## `/node/terminate/`+_node_

```
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "targetRunState": "TERMINATE"
}
```

## `/node/replace/`+_node_

```
{
   "success" : "false",
   "reason" : "Some error message"
}
```


```
{
   "success" : "true",
   "ipToReplace" : "127.0.0.1",
   "hostname" : "localhost",
   "targetRunState": "TERMINATE"
}
```

## `/scale/nodes?nodes=N`

```
{
   "oldNodeCount" : "3",
   "seedNodeCount" : "2",
   "applied" : true,
   "newNodeCount" : 5
}
```

## `/node/seed/`+_node_

```
{
   "ip" : "127.0.0.1",
   "hostname" : "localhost",
   "executorId" : "cassandra.node.1.executor",
   "oldSeedState" : "false",
   "success" : "false",
   "error" : "Some error message"
}
```

```
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