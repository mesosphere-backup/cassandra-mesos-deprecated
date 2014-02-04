package mesosphere.utils

import org.apache.mesos.state.State
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import mesosphere.cassandra.Logger
import com.twitter.chill.KryoInjection
import mesosphere.mesos.util.FrameworkID

/**
 * @author Tobi Knaup
 * @author Erich Nachbar
 */

class StateStore(state: State) extends Logger {

  val defaultWait = Duration(3, "seconds")
  val prefix = "app:"

  def fetch[S](key: String): Option[S] = {
    val variable = state.fetch(prefix + key).get()
    variable.value() match {
      case size if size.length == 0 => None
      case _ => Some(KryoInjection.invert(variable.value).get.asInstanceOf[S])
    }
  }

  def store(key: String, value: Any): Unit = {
    val empty = state.fetch(prefix + key).get()
    val newVar = empty.mutate(KryoInjection(value))
    state.store(newVar).get()
  }

  def expunge(key: String): Boolean = {
    val existingVal = state.fetch(prefix + key).get()
    state.expunge(existingVal).get().booleanValue()
  }

  def names(): Iterator[String] = {
    try {
      state.names().get().asScala
        .filter(_.startsWith(prefix))
        .map(_.replaceFirst(prefix, ""))
    } catch {
      // Thrown when node doesn't exist
      case e: ExecutionException => Seq().iterator
    }
  }

}
