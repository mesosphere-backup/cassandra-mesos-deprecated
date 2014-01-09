package mesosphere.cassandra

import mesosphere.mesos.util.FrameworkInfo
import org.yaml.snakeyaml.Yaml
import java.io.FileReader
import java.util
import scala.collection.JavaConverters._
import org.apache.commons.cli.MissingArgumentException
import java.net.InetAddress
import org.apache.log4j.{Level, BasicConfigurator}

// -Djava.library.path=/usr/local/lib

/*

mesos.executor.uri:
            Once you fill in the configs and repack the distribution, you need to place the distribution somewhere where Mesos executors can find it.

mesos.master.url:
            URL for the Mesos master. Like zk://localhost:2181/mesos

java.library.path:
            Where the mesos lib is installed like /usr/local/lib

 */
object Main extends App with Logger {

  val yaml = new Yaml()
  val mesosConf = yaml.load(new FileReader("conf/mesos.yaml"))
    .asInstanceOf[util.LinkedHashMap[String, Any]].asScala

  // Get configs out of the mesos.yaml file
  val execUri = mesosConf.getOrElse("mesos.executor.uri",
    throw new MissingArgumentException("Please specify the mesos.executor.uri")).toString

  val masterUrl = mesosConf.getOrElse("mesos.master.url",
    throw new MissingArgumentException("Please specify the mesos.master.url")).toString

  val javaLibPath = mesosConf.getOrElse("java.library.path",
    "/usr/local/lib").toString

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

  // Instanciate framework and scheduler
  val framework = FrameworkInfo("CassandraMesos")
  val scheduler = new CassandraScheduler(masterUrl, execUri, confServerHostName, confServerPort, resources)

  new Thread(scheduler).start()
  scheduler.waitUnitInit

  val configServer = new ConfigServer(confServerPort, "conf", scheduler.nodeSet)

  info("Cassandra nodes starting on: " + scheduler.nodeSet.mkString(","))

}
