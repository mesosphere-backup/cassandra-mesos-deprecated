package mesosphere.cassandra

import java.util
import mesosphere.mesos.util.{FrameworkInfo, ScalarResource}
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, MesosSchedulerDriver, SchedulerDriver, Scheduler}
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.CountDownLatch

/**
 * Mesos scheduler for Cassandra
 * Takes care of most of the "annoying things" like distributing binaries and configuration out to the nodes.
 *
 * @author erich<IDonLikeSpam>nachbar.biz
 */
class CassandraScheduler(masterUrl: String,
                         execUri: String,
                         confServerHostName: String,
                         confServerPort: Int,
                         resources: mutable.Map[String, Float],
                         numberOfHwNodes: Int)
  extends Scheduler with Runnable with Logger {

  var initialized = new CountDownLatch(1)

  var nodeSet = mutable.Set[String]()

  def error(driver: SchedulerDriver, message: String) {
    //TODO erich implement
  }

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {
    //TODO erich implement
  }

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {
    //TODO erich implement
  }

  def disconnected(driver: SchedulerDriver) {
    //TODO erich implement
  }

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {
    //TODO erich implement
  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    debug(s"received status update $status")
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) {}

  // Blocks with CountDown latch until we have enough seed nodes.
  def waitUnitInit {
    initialized.await()
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {

    // Construct command to run
    val cmd = CommandInfo.newBuilder
      .addUris(CommandInfo.URI.newBuilder.setValue(execUri))
      .setValue(s"cd cassandra-mesos* && cd conf && rm cassandra.yaml && wget http://${confServerHostName}:${confServerPort}/cassandra.yaml && cd .. && bin/cassandra -f")

    // Create all my resources
    val res = resources.map {
      case (k, v) => ScalarResource(k, v).toProto
    }

    // Let's make sure we don't start multiple Cassandras from the same cluster on the same box.
    // We can't hand out the same port multiple times.
    for (offer <- offers.asScala if isOfferGood(offer) && !haveEnoughNodes()) {
      debug(s"offer $offer")

      info("Accepted offer: " + offer.getHostname)

      val id = "task" + System.currentTimeMillis()

      val task = TaskInfo.newBuilder
        .setCommand(cmd)
        .setName(id)
        .setTaskId(TaskID.newBuilder.setValue(id))
        .addAllResources(res.asJava)
        .setSlaveId(offer.getSlaveId)
        .build

      driver.launchTasks(offer.getId, List(task).asJava)
      nodeSet += offer.getHostname
    }

    // If we have at least one node the assumption is that we are good to go.
    if (nodeSet.size == numberOfHwNodes) initialized.countDown()

  }


  def haveEnoughNodes() = {
    nodeSet.size == numberOfHwNodes
  }

  // Check if offer is reasonable
  def isOfferGood(offer: Offer) = {

    // Make a list of offered resources
    val offeredRes = offer.getResourcesList.asScala.toList.map {
      k => (k.getName, k.getScalar.getValue)
    }

    // Make a list of resources we need
    val requiredRes = resources.toList

    info("resources offered: " + offeredRes)
    info("resources required: " + requiredRes)

    // creates map structure: resourceName, List(offer, required) and
    val resCompList = (offeredRes ++ requiredRes)
      .groupBy(_._1)
      .mapValues(_.map(_._2)
      .toList)

    // throws out resources that have no resource offer or resource requirement
    // counts how many are too small
    val offersTooSmall = resCompList.filter {
      _._2.size > 1
    }.map {
      case (name, values: List[AnyVal]) =>
        values(0).toString.toFloat >= values(1).toString.toFloat
    }.filter {
      !_
    }.size

    // don't start the same framework multiple times on the same host and
    // make sure we got all resources we asked for
    !nodeSet.contains(offer.getHostname) && offersTooSmall == 0
  }

  def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo) {
    //TODO erich implement
  }

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    info(s"Framework registered as ${frameworkId.getValue}")

    val request = Request.newBuilder()
      .addResources(ScalarResource("cpus", 1.0).toProto)
      .build()

    val r = new util.ArrayList[Protos.Request]
    r.add(request)
    driver.requestResources(r)

  }

  def run() {
    info("Starting up...")
    val driver = new MesosSchedulerDriver(this, FrameworkInfo("CassandraMesos").toProto, masterUrl)
    driver.run().getValueDescriptor.getFullName
  }

  //TODO not used yet - we only do Scalar resources as of yet
  def makeRangeResource(name: String, start: Long, end: Long) = {
    Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder()
      .addRange(Value.Range.newBuilder().setBegin(start).setEnd(end)))
      .build
  }

}
