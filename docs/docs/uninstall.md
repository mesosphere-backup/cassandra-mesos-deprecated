---
title: Manual Uninstall
---

Manual Uninstall
================

------------

**DISCLAIMER**
_This is a very early version of Cassandra-on-Mesos framework. This document, code behavior, and anything else may change without notice and/or break older installations._

------------


## Manual Uninstall on DCOS

_All instructions below assume the default values were used when installed on DCOS_

To uninstall the Cassandra DCOS Service complete the following actions:

1. Run `dcos package uninstall cassandra`
2. ZooKeeper Node provided to `CASSANDRA_ZK`
    * Using zkCli run the `rmr /cassandra-mesos/dcos` command
3. Remove the directory provided to `CASSANDRA_DATA_DIRECTORY`
    * The default behavior is to write data into the Mesos Sandbox and does not need to be manually cleaned unless a specific value was provided.
    
    
## Manual Uninstall on Mesos

_Following the instructions will completely uninstall the Cassandra-on-Mesos framework -- data and all, no recovery will be possible._

To uninstall the Cassandra Mesos Framework complete the following actions:

1. Delete Marathon app
2. Shutdown framework
    1. Get framework ID from `http://mesos-master:5050/#/frameworks`
    2. run `curl -d "frameworkId=<framework_id_from_step_1>" http://mesos-master:5050/master/shutdown`
3. ZooKeeper Node provided to `CASSANDRA_ZK`
    * Using zkCli run the `rmr` command
4. Remove the directory provided to `CASSANDRA_DATA_DIRECTORY`
    * The default behavior is to write data into the Mesos Sandbox and does not need to be manually cleaned unless a specific value was provided.
