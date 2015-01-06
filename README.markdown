# Proof of Concept

Please note that this is a proof of concept and should not be used in production.

## Overview
This project allows you to utilize your Mesos cluster to run Cassandra. 
The scheduler (aka the bin/cassandra-mesos process) will do all the heavy lifting like downloading Cassandra to the worker nodes, distributing the configuration and monitoring the instances. It will automatically modify the cassandra.yaml file to include the selected nodes running Cassandra as seed nodes through a template variable. 

## Features
* **Simple**  
 Change the URL to your Mesos master and let Cassandra on Mesos take care of distributing the configuration, install and start Cassandra on your cluster.

* **Multi-cluster aware**  
 Run multiple Cassandra clusters on Mesos. All cassandra directories are namespaced by the sluggified Cassandra cluster name. Just pick different cluster names and different ports for each Cassandra cluster in cassandra.yaml.

* **Easy scaling**  
 Increase the number of hardware nodes Cassandra should run on in mesos.yaml and restart the scheduler. When the scheduler comes up it will add the additional nodes automatically. Please not that scaling down is currently not implemented.

* **Robust**  
 The scheduler will automatically restart terminated Cassandra instances on other Mesos nodes. If the scheduler dies the nodes will continue to run for 1 week before the Cassandra nodes will shut down. The scheduler will automatically reconnect to a running cluster after a restart.

## Production Deployment Notes
Please make sure to use absolute paths in the cassandra.yaml config file to speed up booting of nodes by using existing data if possible. The variable ```${clusterName}``` can be used in the config file to include a sluggified cluster name as part of the path. Check the included cassandra.yaml config file for an example.

## Tutorial
If you are running a local Mesos install on your machine the default settings will work for you. Just start the scheduler as shown in the last step.

**Steps:**    

1. Download the distribution from the Mesosphere [download server](http://downloads.mesosphere.io/cassandra/cassandra-mesos-2.0.5-1.tgz).

1. Untar it onto the scheduler machine   
   ```tar xzf cassandra-mesos-*.tgz```

1. Edit ```conf/mesos.yaml``` and replace it with your Mesos settings.

1. Edit ```conf/cassandra.yaml``` and replace it with your Cassandra settings.

1. Start the scheduler to initiate launching Cassandra on Mesos    
   <code>bin/cassandra-mesos<code>

## Configuration Values

### mesos.executor.uri
Adjust this if you want the nodes to retrieve the distribution from somewhere else. Please note that it is typically not required to change anything inside the tarball. The configuration is pushed out from the scheduler to all the nodes automatically for you.

Default: ```http://downloads.mesosphere.io/cassandra/cassandra-mesos-2.0.5-1.tgz```

#### mesos.master.url  
Change this setting to point to your Mesos Master. The default works for a local Mesos install.

Default: ```zk://localhost:2181/mesos```

#### state.zk
Which Zookeeper servers to use to store our state (e.g.'zkHost0:port,zkHost1:port'). The system will store all its state under ```/cassandraMesos/[cassandra cluster name]``` node. 

Default: ```localhost:2181```


#### java.library.path
Change this to the directory where the mesos libraries are installed.

Default: ```/usr/local/lib```

#### cassandra.noOfHwNodes
How many hardware nodes we want to run this Cassandra cluster on. Cassandra requires to have the same ports for all of its cluster members. This prevents multiple nodes from the same Cassandra cluster to run on a single physical node. Scaling up is achieved by increasing the number and restarting the scheduler process.

Default: ```1```

#### cassandra.minNoOfSeedNodes
How many minimum no of seed nodes we want to configure when running Cassandra cluster. Make sure it is less or equal cassandra.noOfHwNodes

Default: ```1```

#### resource.*
The specified resources will be relayed to Mesos to find suitable machines. The configuration file lists ```cpu```, ```mem``` and ```disk```, but really anything you specify will be relayed to Mesos as a scalar value when requesting resources.

Defaults:  ```resource.cpu:0.1```, ```resource.mem: 2048```, ```resource.disk: 1000```

## Building

Execute ```./build.sh``` to download all dependencies including Cassandra, compile the code and make the distribution. 

## Known Limitations

Currently the scheduler does not deal with cluster failure in an intelligent manner. These features will be added shortly once we gain some initial feedback.

## Versioning

Cassandra-Mesos uses the version of the embedded Cassandra for the first 3 version numbers. The last and 4th version number is the version of Cassandra-Mesos.




