package mesosphere.cassandra

import org.yaml.snakeyaml.Yaml
import java.io.FileReader
import java.util
import scala.collection.JavaConverters._
import org.apache.commons.cli.MissingArgumentException
import java.net.{URI, InetAddress}
import org.apache.log4j.{Level, BasicConfigurator}
import mesosphere.utils.{StateStore, Slug}
import org.apache.mesos.state.ZooKeeperState
import java.util.concurrent.TimeUnit

/**
 * Mesos on Cassandra
 * Takes care of most of the "annoying things" like distributing binaries and configuration out to the nodes.
 *
 * @author erich<IDonLikeSpam>nachbar.biz
 */
object Main extends App with Logger {

  val yaml = new Yaml()
  val mesosConf = yaml.load(new FileReader("conf/mesos.yaml"))
    .asInstanceOf[util.LinkedHashMap[String, Any]].asScala

  val cassConf = yaml.load(new FileReader("conf/cassandra.yaml"))
    .asInstanceOf[util.LinkedHashMap[String, Any]].asScala

  // Get configs out of the mesos.yaml file
  val execUri = mesosConf.getOrElse("mesos.executor.uri",
    throw new MissingArgumentException("Please specify the mesos.executor.uri")).toString

  val masterUrl = mesosConf.getOrElse("mesos.master.url",
    throw new MissingArgumentException("Please specify the mesos.master.url")).toString

  val javaLibPath = mesosConf.getOrElse("java.library.path",
    "/usr/local/lib/libmesos.so").toString
  System.setProperty("java.library.path", javaLibPath)

  val numberOfHwNodes = mesosConf.getOrElse("cassandra.noOfHwNodes", 1).toString.toInt
  
  val numberOfSeedNodes = mesosConf.getOrElse("cassandra.noOfSeedNodes", 1).toString.toInt

  val zkStateServers = mesosConf.getOrElse("state.zk", "localhost:2181/cassandra-mesos").toString

  val confServerPort = mesosConf.getOrElse("cassandra.confServer.port", 8282).toString.toInt

  val confServerHostName = mesosConf.getOrElse("cassandra.confServer.hostname",
    InetAddress.getLocalHost().getHostName()).toString

  // Find all resource.* settings in mesos.yaml and prep them for submission to Mesos
  val resources = mesosConf.filter {
    _._1.startsWith("resource.")
  }.map {
    case (k, v) => k.replaceAllLiterally("resource.", "") -> v.toString.toFloat
  }

  //TODO erich load the Cassandra log4j-server.properties file
  BasicConfigurator.configure()
  getRootLogger.setLevel(Level.INFO)

  info("Starting Cassandra on Mesos.")

  // Get the cluster name out of the cassandra.yaml
  val clusterName = cassConf.get("cluster_name").get.toString
  val state = new ZooKeeperState(
    zkStateServers,
    20000,
    TimeUnit.MILLISECONDS,
    "/cassandraMesos/" + Slug(clusterName)
  )


  val store = new StateStore(state)

  // Instanciate framework and scheduler
  val scheduler = new CassandraScheduler(masterUrl,
    execUri,
    confServerHostName,
    confServerPort,
    resources,
    numberOfHwNodes,
    numberOfSeedNodes,
    clusterName)(store)

  val schedThred = new Thread(scheduler)
  schedThred.start()
  scheduler.waitUnitInit

  // Start serving the Cassandra config
  val configServer = new ConfigServer(confServerPort, "conf", scheduler.getHosts(), Slug(clusterName))

  info("Cassandra nodes starting on: " + scheduler.fetchNodeSet().mkString(","))

}

