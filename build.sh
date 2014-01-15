#!/bin/bash -v

# Our cassandra-mesos project version follows the Cassandra version number
PROJVERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[')
CASSVERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=cassandra.version | grep -v '\[')

echo Building Cassandra $CASSVERSION for Mesos

# Create our jar so we can package it up as well. Do this first, so we can fail fast
mvn clean package

rm -r cassandra-mesos-*
wget http://www.webhostingjams.com/mirror/apache/cassandra/${CASSVERSION}/apache-cassandra-${CASSVERSION}-bin.tar.gz

tar xzf apache-cassandra*.tar.gz
rm apache-cassandra*tar.gz

mv apache-cassandra* cassandra-mesos-${PROJVERSION}

cp bin/cassandra-mesos cassandra-mesos-${PROJVERSION}/bin
chmod u+x cassandra-mesos-${PROJVERSION}/bin/cassandra-mesos

cp conf/* cassandra-mesos-${PROJVERSION}/conf
cp target/*.jar cassandra-mesos*/lib

tar czf cassandra-mesos-${PROJVERSION}.tgz cassandra-mesos-${PROJVERSION}


