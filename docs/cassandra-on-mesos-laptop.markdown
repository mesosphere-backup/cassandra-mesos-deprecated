Cassandra-on-Mesos on a Laptop
==============================

------------

**DISCLAIMER**
_You are looking at a very early version of Cassandra-on-Meos framework. Things mentioned in this
document, behavior implemented in the code and anything else may change without notice and/or break older installations._

------------

It is quite easy to setup and try Cassandra-on-Mesos on your personal Linux box or Mac. Windows is *not* supported -
sorry guys. Note that production configurations only support Linux machines - but it's perfectly valid to use a
Mac for testing and/or development.

# Things needed

* [Apache ZooKeeper] version 3.4.6 or newer
* [Apache Mesos] version 0.22 or newer
* [Apache Cassandra] version 2.1.x
* [Cassandra-on-Mesos] checkout the `master` branch (`git clone https://github.com/mesosphere/cassandra-mesos.git`)
* Oracle JDK 1.7.0u76 or newer

# Mac note

If you want to run multiple slaves using local IP addresses like `127.0.0.1`, `127.0.0.2` etc you have to make
them available first:

```
sudo ifconfig lo0 alias 127.0.0.2 up
sudo ifconfig lo0 alias 127.0.0.3 up
sudo ifconfig lo0 alias 127.0.0.4 up
```

# Configuring and starting the stuff

For convenience, this document assumes that you have created a directory `mkdir ~/cassandra-on-mesos` and your
working directory is that directory `cd ~/cassandra-on-mesos`.

## ZooKeeper

1. Create a directory `zookeeper` and `cd` into it.
1. Unpack zookeeper tarball here
1. Create a directory `data`
1. The directory should look like this now:
   ```
   ~/cassandra-on-mesos/zookeeper$ ls -1
   zookeeper-3.4.6/
   data/
   ```
1. Create the file `zoo.cfg` in `zookeeper-3.4.6/conf`:
   ```
   tickTime=2000
   dataDir=/home/me/zookeeper/data
   clientPort=2181
   ```
   Replace the `dataDir` value with the one matching your installation.
1. Start ZooKeeper
   ```
   ~/cassandra-on-mesos/zookeeper$ cd zookeeper-3.4.6
   ~/cassandra-on-mesos/zookeeper/zookeeper-3.4.6$ bin/zkServer.sh start
   ```

## Mesos

1. Create a directory `mesos`and `cd` into it
1. If you've built Mesos from source, go into the `build` directory
1. Define a base working directory for the master and the slave(s) - for example `/tmp/mesos`
   (use `/private/tmp/mesos` on Mac)
1. Now start a bunch of processes - one Mesos master and at least one Mesos slave
   ```
   MY_IP=127.0.0.1
   MY_IP_2=127.0.0.2
   MY_IP_3=127.0.0.3
   BASEDIR=/tmp/mesos
   ./bin/mesos-master.sh --ip=${MY_IP} --work_dir=${BASEDIR}/master --zk=zk://$MY_IP:2181/mesos --quorum=1 &
   ./bin/mesos-slave.sh --master=${MY_IP}:5050 --ip=${MY_IP} --work_dir=${BASEDIR}/slave1 --resources='ports:[31000-32000,7000-7001,7199-7199,9042-9042,9160-9160]' &
   ./bin/mesos-slave.sh --master=${MY_IP}:5050 --ip=${MY_IP_2} --work_dir=${BASEDIR}/slave2 --resources='ports:[31000-32000,7000-7001,7199-7199,9042-9042,9160-9160]' &
   ./bin/mesos-slave.sh --master=${MY_IP}:5050 --ip=${MY_IP_3} --work_dir=${BASEDIR}/slave3 --resources='ports:[31000-32000,7000-7001,7199-7199,9042-9042,9160-9160]' &
   ```
1. Check that Mesos is running by opening `http://127.0.0.1:5050/` in your browser

## Cassandra-on-Mesos

### Cassandra-on-Mesos from a shell prompt

1. Open the file `dev-run.bash` and update the variables when necessary.
1. Execute `dev-run.bash`

### Cassandra-on-Mesos from an IDE

1. Basically you need to `git clone https://github.com/mesosphere/cassandra-mesos.git`
1. Open your IDE, create a project and import it using the Maven model.
1. Define some environment variables in the run configuration of your IDE for
   `io.mesosphere.mesos.frameworks.cassandra.Main`:
   ```
   # Number of Cassandra nodes to start
   export CASSANDRA_NODE_COUNT=2
   # Number of seed nodes
   CASSANDRA_SEED_COUNT=1
   # Nice to change the default value of 60 seconds to 10 seconds on development systems
   CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS=10
   # Nice to change the default value of 120 seconds to 0 for local test and development systems
   CASSANDRA_BOOTSTRAP_GRACE_TIME_SECONDS=0
   # The port on which the REST API will be available
   PORT0=18080
   # Absolute path where libmesos is 
   MESOS_NATIVE_JAVA_LIBRARY=.../libmesos.dylib
   # Absolute path where the Cassandra-on-Mesos executor is
   EXECUTOR_FILE_PATH=.../cassandra-executor-0.1.0-SNAPSHOT-jar-with-dependencies.jar
   # Absolute path where you've downloaded the JRE
   JRE_FILE_PATH=...jre-7u76-macosx-x64.tar.gz
   # Absolute path where you've downloaded Apache Cassandra
   CASSANDRA_FILE_PATH=.../apache-cassandra-2.1.2-bin.tar.gz
   ```

# Start from scratch

1. Kill all java processes (make sure you don't accidentally kill your IDE's processes).
1. Kill all mesos processes, scrub the base working directory
1. Stop ZooKeeper, scrub the data directory
1. Start ZooKeeper, Mesos master, Mesos slave(s), Cassandra-on-Mesos framework 



[Apache Cassandra]: http://cassandra.apache.org/
[Apache Mesos]: http://mesos.apache.org/
[Apache ZooKeeper]: http://zookeeper.apache.org/
[Cassandra-on-Mesos]: https://github.com/mesosphere/cassandra-mesos/
