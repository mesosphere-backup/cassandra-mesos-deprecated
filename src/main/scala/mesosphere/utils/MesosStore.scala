package mesosphere.utils

import org.apache.mesos.state.State
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import com.sun.xml.internal.messaging.saaj.util.{ByteInputStream, ByteOutputStream}
import java.io.{ObjectInputStream, ObjectOutputStream}
import scala.pickling._
import mesosphere.cassandra.Logger
import json._

/**
 * @author Tobi Knaup
 * @author Erich Nachbar
 */

//class MesosStore[S](state: State) extends PersistenceStore[S] {
class MesosStore[S](state: State) extends Logger {

  val defaultWait = Duration(3, "seconds")
  val prefix = "app:"

  import ExecutionContext.Implicits.global
  import mesosphere.util.BackToTheFuture.FutureToFutureOption

  def fetch(key: String): Option[String] = {
    val variable = state.fetch(prefix + key).get()
    Option(new String(variable.value).unpickle[String])
  }

  def store(key: String, value: String): Option[String] = {
    val empty = state.fetch(prefix + key).get()
    val newVar = empty.mutate(value.pickle.value.getBytes())
    val stored = state.store(newVar)
//    map {
//      //TODO should we do this deserialization or just give back the one we got in? Looks like we could save some effort...
//      case Some(newVar) => Some(value)
//      case None => throw new StorageException(s"Failed to store $key")
//    }
    Option(value)
  }

  def expunge(key: String): Future[Boolean] = {
    state.fetch(prefix + key) flatMap {
      case Some(variable) =>
        state.expunge(variable) map {
          case Some(b) => b
          case None => throw new StorageException(s"Failed to expunge $key")
        }
      case None => throw new StorageException(s"Failed to read $key")
    }
  }

  def names(): Future[Iterator[String]] = {
    // TODO use implicit conversion after it has been merged
    future {
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

}
