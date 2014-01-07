package mesosphere.cassandra

import java.util
import mesosphere.mesos.util.{FrameworkInfo, ScalarResource}
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, MesosSchedulerDriver, SchedulerDriver, Scheduler}
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import java.net.InetAddress

/**
 * TODO write header
 *
 */

//TODO erich - how to prevent multiple cassandras running on the same host? ports need to be the same across all machines
//TODO erich - use resourceStrings from config
class CassandraScheduler(masterUrl :String, execUri :String, confServerHostName :String ,confServerPort :Int) extends Scheduler with Runnable {

  var nodeSet = mutable.Set[String]()
  var initialized = new CountDownLatch(1)

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    println(s"received status update $status")
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) {}

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    for (offer <- offers.asScala) {
      println(s"offer $offer")

      val cmd = CommandInfo.newBuilder
        .addUris(CommandInfo.URI.newBuilder.setValue(execUri))
        .setValue(s"cd cassandra-mesos* && cd conf && rm cassandra.yaml && http://${confServerHostName}:${confServerPort}/cassandra.yaml && cd .. && bin/cassandra -f")

      val cpus = ScalarResource("cpus", 1.0)
//      val port = ScalarResource("port", 9900) //didn't work, added by enachb
      val id = "task" + System.currentTimeMillis()

      val task = TaskInfo.newBuilder
        .setCommand(cmd)
        .setName(id)
        .setTaskId(TaskID.newBuilder.setValue(id))
        .addResources(cpus.toProto)
        //        .addResources(port.toProto)
        .setSlaveId(offer.getSlaveId)
        .build

      driver.launchTasks(offer.getId, List(task).asJava)
      nodeSet += offer.getHostname
      initialized.countDown()

    }
  }

  def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo) {}

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {

    println(s"registered as ${frameworkId.getValue}")

//    val resources = Resource.newBuilder().setScalar(ScalarResource("cpus", 1.0).toProto)
//    val request = Request.newBuilder().addResources(ScalarResource("cpus", 1.0).toProto).addResources(ScalarResource("port", 9900, "*").toProto).build()

    //TODO erich add other resources too so we don't start many Cassandra nodes on one box
    val request = Request.newBuilder().addResources(ScalarResource("cpus", 1.0).toProto).build()
    val r = new util.ArrayList[Protos.Request]
    r.add(request)
    driver.requestResources(r)

  }

  //TODO implement
  def run() {
  //  log.info("Starting up...")
    val driver = new MesosSchedulerDriver(this, FrameworkInfo("CassandraMesos").toProto, masterUrl)
    val status = driver.run().getValueDescriptor.getFullName
//    log.info(s"Final status: $status")
  }

}
