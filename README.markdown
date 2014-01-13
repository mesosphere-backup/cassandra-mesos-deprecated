## Overview
This project allows you to utilize your Mesos cluster to run Cassandra. 
The driver will do all the heavy lifting like downloading Cassandra to the worker nodes, distributing the configuration and monitoring the instances. It will automatically modify the cassandra.yaml file to include the selected nodes running Cassnandra as seed nodes through a template variable. 

> Please note that this is a beta release and there is still work left to do cover all failure scenarios that can happen.

## Tutorial
If you are running a local Mesos install on your machine the default settings will work for you. Just start the driver as shown in the last step.

**Steps:**    

1. Download the distribution from the Mesosphere [download server](http://downloads.mesosphere.io/storm/cassandra-mesos-2.0.3.tgz).

1. Untar it onto the driver machine   
   ```tar xzf cassandra-mesos-*.tgz```

1. Edit ```conf/mesos.yaml``` and replace it with your Mesos settings.

1. Edit ```conf/cassandra.yaml``` and replace it with your Cassandra settings.

1. Start the driver to initiate launching Cassandra on Mesos    
   ```bin/cassandra-mesos```

## Configuration Values

### mesos.executor.uri
> Adjust this if you want the nodes to retrieve the distribution from somewhere else   

> Default: ```http://downloads.mesosphere.io/storm/cassandra-mesos-2.0.3.tgz```

#### mesos.master.url  
> Change this setting to point to your Mesos Master. The default works for a local Mesos install.   

> Default: ```zk://localhost:2181/mesos```

#### java.library.path
> Change this to the directory where the mesos libraries are installed.    

> Default: ```/usr/local/lib```

#### resource.*
> The specified resources will be relayed to Mesos to find suitable machines. The configuration file lists ```cpu```, ```mem``` and ```disk```, but really anything you specify will be relayed to Mesos as a scalar value when requesting resources.    

> Defaults:  ```resource.cpu:0.1```, ```resource.mem: 2048```, ```resource.disk: 1000```

## Building

Execute ```./build.sh``` to download all dependencies including Cassandra, compile the code and make the distribution. 

## Known Limitations

Currently the driver does not deal with cluster failure in an intelligent manner. These features will be added shortly once we gain some initial feedback.

## Versioning

Cassandra-Mesos uses the version of the embedded Cassandra as the first 3 version numbers. The last and 4th version number is the version of Cassandra-Mesos.




