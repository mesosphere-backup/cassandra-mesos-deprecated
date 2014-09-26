package mesosphere.cassandra

import java.util
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver, Scheduler}
import scala.collection.JavaConverters._
import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import mesosphere.utils.{TaskIDUtil, StateStore}
import scala.collection.mutable._

/**
 * Mesos scheduler for Cassandra
 * Takes care of most of the "annoying things" like distributing binaries and configuration out to the nodes.
 *
 * @author erich<IDonLikeSpam>nachbar.biz
 */

case class TaskInfoContainer(taskId: String, hostname: String)

class CassandraScheduler(masterUrl: String,
                         execUri: String,
                         confServerHostName: String,
                         confServerPort: Int,
                         resources: Map[String, Float],
                         numberOfHwNodes: Int,
                         numberOfSeedNodes: Int,
                         clusterName: String)
                        (implicit val store: StateStore)
  extends Scheduler with Runnable with Logger {

  val FRMWIDKEY = "frameworkId"

  var initialized = new CountDownLatch(1)

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
    info("Received status update for task %s: %s (%s)"
      .format(status.getTaskId.getValue, status.getState, status.getMessage))

    if (status.getState.eq(TaskState.TASK_FAILED)
      || status.getState.eq(TaskState.TASK_FINISHED)
      || status.getState.eq(TaskState.TASK_KILLED)
      || status.getState.eq(TaskState.TASK_LOST)) {

      // Remove from our internal list
      removeNode(status.getTaskId)
      info(s"Removing task ${status.getTaskId} from internal task list.")

    } else if (status.getState.eq(TaskState.TASK_RUNNING)) {
      info(s"Got update that task ${status.getTaskId} is running.")

    } else if (status.getState.eq(TaskState.TASK_STAGING)) {
      info(s"Task ${status.getTaskId} is being staged.")

    } else {
       //TODO erich Do I need anything else here?
     }
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) {}

  // Blocks with CountDown latch until we have enough seed nodes.
  def waitUnitInit {
    initialized.await()
  }

  def fetchNodeSet() : Set[TaskInfoContainer] = {
    store.fetch[Set[TaskInfoContainer]]("nodeSet").getOrElse(Set[TaskInfoContainer]())
  }

  def saveNodeSet(nodeSet: Set[TaskInfoContainer]) : Set[TaskInfoContainer]= {
    store.store("nodeSet", nodeSet)
    nodeSet
  }

  def isHostRunning(hostname: String): Boolean = {
    fetchNodeSet().filter {
      _.hostname == hostname
    }.size > 0
  }

  def removeNode(hostname: String) : Set[TaskInfoContainer] = {
    val updated = fetchNodeSet().filter {
      _.hostname != hostname
    }
    saveNodeSet(updated)
    updated
  }

  def removeNode(taskId: TaskID) : Set[TaskInfoContainer]= {
    val updated = fetchNodeSet().filter {
      _.taskId != taskId.getValue
    };
    saveNodeSet(updated)
    updated
  }

  def getHosts(): Set[String] = {
    fetchNodeSet().map {
      _.hostname
    }
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {

    val offeredHosts = offers.asScala.map{_.getHostname}
    info(s"Got new resource offers ${offeredHosts}")
    // Pull back in previous nodes so we can prefer them and reuse the data files on disk
    var nodes = fetchNodeSet()

    // Construct command to run
    val cmd = CommandInfo.newBuilder
      .addUris(CommandInfo.URI.newBuilder.setValue(execUri))
      .setValue(s"cd cassandra-mesos* && cd conf && rm cassandra.yaml && wget http://${confServerHostName}:${confServerPort}/cassandra.yaml && cd .. && bin/cassandra -f")

    // Create all my resources
    val res = resources.map {
      case (k, v) => Resource.newBuilder()
        .setName(k)
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder().setValue(v).build())
        .build()
    }

    // Let's make sure we don't start multiple Cassandras from the same cluster on the same box.
    // We can't hand out the same port multiple times.
    for (offer <- offers.asScala.sortBy(
      // Prefer existing Cassandra servers over new ones so we can reuse the data on disk
      // if the node doesn't meet the set criteria e.g. lack of diskspace, cpu, ... it will get kicked out later
      o => !isHostRunning(o.getHostname)
    )) {

      if (isOfferGood(offer, nodes) && !haveEnoughNodes(nodes.size)) {
        // Accepting offer
        debug(s"offer $offer")

        info("Accepted offer: " + offer.getHostname)

        val id = s"cassandra_${System.currentTimeMillis()}"

        val task = TaskInfo.newBuilder
          .setCommand(cmd)
          .setName(id)
          .setTaskId(TaskID.newBuilder.setValue(id))
          .addAllResources(res.asJava)
          .setSlaveId(offer.getSlaveId)
          .build

        driver.launchTasks(List(offer.getId).asJava, List(task).asJava)
        nodes += TaskInfoContainer(task.getTaskId.getValue, offer.getHostname)
        saveNodeSet(nodes)

      } else {
        // Declining offer
        driver.declineOffer(offer.getId)
      }
    }

    // If we have enough nodes we are good to go
    if (haveEnoughSeedNodes(nodes.size)) initialized.countDown()

  }


  def haveEnoughNodes(noOfNodes: Int) = {
    noOfNodes == numberOfHwNodes
  }
  
  def haveEnoughSeedNodes(noOfNodes: Int) = {
    noOfNodes == numberOfSeedNodes
  }

  // Check if offer is reasonable
  def isOfferGood(offer: Offer, nodeSet: Set[TaskInfoContainer]) = {

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
    !isHostRunning(offer.getHostname) && offersTooSmall == 0
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo) {
    info("Re-registered to %s".format(master))
  }

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    info(s"Framework registered as ${frameworkId.getValue}")

    store.store(FRMWIDKEY, frameworkId.toByteArray)
  }

  def run() {
    info("Starting up...")

    val frameworkInfo = FrameworkInfo.newBuilder()
      .setName("Cassandra " + clusterName)
      .setFailoverTimeout(7.days.toSeconds)
      .setUser("") // Let Mesos assign the user
    //      .setCheckpoint(config.checkpoint()) //TODO erich should we use this?

    // Pull an existing framework Id if we have one stored
    store.fetch(FRMWIDKEY) match {
      case Some(id) =>
        val existingId = FrameworkID.parseFrom(store.fetch[Array[Byte]](FRMWIDKEY).get)
        frameworkInfo.setId(existingId)
      case None =>
        info("Starting Cassandra cluster ${clusterName} for the first time. Allocating new ID for it.")
    }

    val driver = new MesosSchedulerDriver(this, frameworkInfo.build(), masterUrl)
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
