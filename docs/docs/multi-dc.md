---
title: Multi Datacenter
---

# Multi Datacenter

Beginning with Cassandra on Mesos version 0.2.0, Cassandra on Mesos can be configured so that multiple datacenters are aware of each other.

See [Deploying Cassandra across Multiple Data Centers](http://www.datastax.com/dev/blog/deploying-cassandra-across-multiple-data-centers) for motivations regarding running Cassandra in multiple datacenters.

## How it works

Cassandra on Mesos can be configured to provide datacenter and rack information to Cassandra upon starting each server.

Cassandra on Mesos can also be configured to query other Cassandra instances to provide seed sets across multiple clusters so that each of the clusters can coordinate and replicate data.


## Requirements

1. Every cluster must be able to reach the IP addresses of every other cluster.
2. All clusters must have the same cluster name.
3. Datacenters must be defined before starting the framework. New datacenters cannot be added after a cluster has initialized.
4. Node sizes and counts must be the same across all clusters. **Note:** Cassandra on Mesos does not enforce or validate this, so be certain that this is configured correctly.

## Configuration

### DC and Rack

Configuration is provided via environment variables.

```
# Value to be used as the default DC name
CASSANDRA_DEFAULT_DC=DC1

# Value to be used as the default RACK name
CASSANDRA_DEFAULT_RACK=RAC1

```

### Other instances of Cassandra on Mesos

Cassandra on Mesos is informed of other Cassandra on Mesos instances running in other Mesos clusters by
environment variables in the format `CASSANDRA_EXTERNAL_DC_<dcName>=<cassandraOnMesosUrl>`.

## Example on Mesos

In this example, three datacenters dc0, dc1, dc2 are running three different Mesos clusters.

The following configuration would be used for each instance of Cassandra on Mesos:

##### dc0
```
CASSANDRA_CLUSTER_NAME=multi-dc
CASSANDRA_DEFAULT_DC=dc0
CASSANDRA_EXTERNAL_DC_dc1=http://multi-dc.cassandra.marathon.mesos1:10001
CASSANDRA_EXTERNAL_DC_dc2=http://multi-dc.cassandra.marathon.mesos2:10002
```

##### dc1
```
CASSANDRA_CLUSTER_NAME=multi-dc
CASSANDRA_DEFAULT_DC=dc1
CASSANDRA_EXTERNAL_DC_dc0=http://multi-dc.cassandra.marathon.mesos0:10000
CASSANDRA_EXTERNAL_DC_dc2=http://multi-dc.cassandra.marathon.mesos2:10002
```

##### dc2
```
CASSANDRA_CLUSTER_NAME=multi-dc
CASSANDRA_DEFAULT_DC=dc2
CASSANDRA_EXTERNAL_DC_dc0=http://multi-dc.cassandra.marathon.mesos0:10000
CASSANDRA_EXTERNAL_DC_dc1=http://multi-dc.cassandra.marathon.mesos1:10001
```

1. Ensure there are TCP routes allowing for traffic flow between each of the Mesos clusters.
2. Start each cluster with the necessary configuration.
3. Wait for each cluster to allocate its seed nodes.
4. After the seeds have been allocated, restart all of the nodes that have already been started. Send a `POST` to the http API at `/cluster/rolling-restart/start` and the framework will restart each of the nodes so that the new seed set will take effect.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Your clusters should now be able to replicate data between each other.

## Example on DCOS

In this example, three datacenters dcos0, dcos1, dcos2 are running three different DCOS clusters.

To install each instance of Cassandra on DCOS using the DCOS CLI:

1. On DCOS Cluster 0, install with `dcos package install cassandra --options=dcos0.json`

    ```json
    {
      "cassandra": {
        "cluster-name": "multi-dc",
        "resources": {
          "cpus": 1.0,
          "mem": 2048,
          "disk": 6144
        },
        "dc": {
          "default-dc": "dcos0",
          "default-rack": "rack0",
          "external-dcs": [
            {
              "name": "dcos1",
              "url": "http://dcos1/service/cassandra.multi-dc"
            },
            {
              "name": "dcos2",
              "url": "http://dcos2/service/cassandra.multi-dc"
            }
          ]
        }
      }
    }
    ```

2. On DCOS Cluster 1, install with `dcos package install cassandra --options=dcos1.json`

    ```json
    {
      "cassandra": {
        "cluster-name": "multi-dc",
        "resources": {
          "cpus": 1.0,
          "mem": 2048,
          "disk": 6144
        },
        "dc": {
          "default-dc": "dcos1",
          "default-rack": "rack1",
          "external-dcs": [
            {
              "name": "dcos0",
              "url": "http://dcos0/service/cassandra.multi-dc"
            },
            {
              "name": "dcos2",
              "url": "http://dcos2/service/cassandra.multi-dc"
            }
          ]
        }
      }
    }
    ```

3. On DCOS Cluster 2 install with `dcos package install cassandra --options=dcos2.json`

    ```json
    {
      "cassandra": {
        "cluster-name": "multi-dc",
        "resources": {
          "cpus": 1.0,
          "mem": 2048,
          "disk": 6144
        },
        "dc": {
          "default-dc": "dcos2",
          "default-rack": "rack2",
          "external-dcs": [
            {
              "name": "dcos0",
              "url": "http://dcos0/service/cassandra.multi-dc"
            },
            {
              "name": "dcos1",
              "url": "http://dcos1/service/cassandra.multi-dc"
            }
          ]
        }
      }
    }
    ```

4. Wait for each cluster to allocate its seed nodes.
5. After the seeds have been allocated, restart all of the nodes that have already been started. Send a `POST` to the http API at `/cluster/rolling-restart/start` and the framework will restart each of the nodes so that the new seed set will take effect.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Your clusters should now be able to replicate data between each other.
