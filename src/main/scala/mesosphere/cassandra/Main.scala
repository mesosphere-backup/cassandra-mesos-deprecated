package mesosphere.cassandra

import org.apache.mesos.MesosSchedulerDriver
import mesosphere.mesos.util.FrameworkInfo
import org.yaml.snakeyaml.Yaml
import java.io.{FileReader, File}
import java.util
import scala.collection.JavaConverters._
import org.apache.commons.cli.MissingArgumentException
import java.net.InetAddress

// -Djava.library.path=/usr/local/lib

/*

mesos.executor.uri:
            Once you fill in the configs and repack the distribution, you need to place the distribution somewhere where Mesos executors can find it.

mesos.master.url:
            URL for the Mesos master. Like zk://localhost:2181/mesos

java.library.path:
            Where the mesos lib is installed like /usr/local/lib

 */
object Main extends App {

  val yaml = new Yaml();
  val mesosConf = yaml.load(new FileReader("conf/mesos.yaml")).asInstanceOf[util.LinkedHashMap[String,String]].asScala

  //TODO erich could be thrown out
//  mesosConf.foreach{case(k:String,v) => System.setProperty(k,v)}

  // Get configs out of the mesos.yaml file
  val execUri = mesosConf.getOrElse("mesos.executor.uri", throw new MissingArgumentException("Please specify the mesos.executor.uri"))
  val masterUrl = mesosConf.getOrElse("mesos.master.url", throw new MissingArgumentException("Please specify the mesos.master.url"))
  val javaLibPath = mesosConf.getOrElse("java.library.path","/usr/local/lib")
  val confServerPort = mesosConf.getOrElse("cassandra.confServer.port", "8282").toInt
  val confServerHostName = mesosConf.getOrElse("cassandra.confServer.hostname", InetAddress.getLocalHost().getHostName())

  val framework = FrameworkInfo("CassandraMesos")

  //TODO hand in resources like cpus,mem & ports as a Map[String,Any] so we can have lists of things in a generic way
  val scheduler = new CassandraScheduler(masterUrl, execUri, confServerHostName ,confServerPort)

  new Thread(scheduler).start()
  scheduler.initialized.await()

  val configServer = new ConfigServer(confServerPort, "conf", scheduler.nodeSet)

}

object Config {



}
