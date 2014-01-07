#!/bin/bash -v

# Our cassandra-mesos project version follows the Cassandra version number
VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[')

echo Building Cassandra $VERSION for Mesos

# Create our jar so we can package it up as well. Do this first, so we can fail fast
mvn clean package

rm -r cassandra-mesos-*
wget http://www.webhostingjams.com/mirror/apache/cassandra/${VERSION}/apache-cassandra-${VERSION}-bin.tar.gz

tar xvzf apache-cassandra*.tar.gz
rm apache-cassandra*tar.gz

mv apache-cassandra* cassandra-mesos-${VERSION}

cp conf/* cassandra-mesos-${VERSION}/conf/
cp target/cassandra-mesos-${VERSION}.jar cassandra-mesos*/lib

tar czf cassandra-mesos-${VERSION}.tgz cassandra-mesos-${VERSION}




