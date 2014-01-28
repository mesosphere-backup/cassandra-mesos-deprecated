//package mesosphere.cassandra
//
//import scala.collection.mutable
//import scala.{collection, Serializable}
//import org.apache.mesos.state.{Variable, State}
//import scala.collection.JavaConverters._
//import java.io.{ObjectOutputStream, ObjectInputStream}
//import com.sun.xml.internal.messaging.saaj.util.{ByteOutputStream, ByteInputStream}
//import mesosphere.utils.StorageException
//
//import scala.concurrent._
//import scala.concurrent.duration._
//
//
///**
//
//  */
//object KVStoreMap extends KVStore {
//
//  val store = mutable.HashMap[String, Serializable]()
//
//  def put(key: String, value: Serializable) = store.put(key, value)
//
//  def get[S <: Serializable](key: String) = store.getOrElse(key, None).asInstanceOf[S]
//
//  def contains(key: String) = store.contains(key)
//
//  def expunge(key: String): Unit = store.remove(key)
//}
//
//trait KVStore {
//
//  def put(key: String, value: Serializable)
//
//  def get[S <: Serializable](key: String): S
//
//  def contains(key: String): Boolean
//
//  def expunge(key: String): Unit
//}
//
//class MesosStore[T <: Serializable](state: State, prefix: String) extends scala.collection.mutable.Map[String, T] {
//
//  import mesosphere.util.BackToTheFuture.FutureToFutureOption
//
//  // TODO not very efficient for large collections
//  def iterator: Iterator[(String, T)] = state.names().get().asScala
//    .map {
//    key =>
//      val bytes = state.fetch(key).get().value()
//      (key, new ObjectInputStream(new ByteInputStream(bytes, bytes.length)).readObject().asInstanceOf[T])
//  }.toIterator
//
////  def +=(kv: (String, T)): MesosStore[T] = {
////    case (key: String, v: T) =>
////      state.fetch(prefix + key).flatMap {
////        case Some(variable)  =>
////          val bytes = new ByteOutputStream()
////          val out = new ObjectOutputStream(bytes)
////          out.writeObject(v)
////          out.close()
////          state.store(variable.mutate(bytes.getBytes)). map {
////            case Some(newVar) => this
////            case None => throw new StorageException(s"Failed to store $key")
////          }
////        case None => throw new StorageException(s"Failed to read $key")
////      }
////  }
//
////  def -=(key: String): MesosStore[T] = {
////    state.fetch(prefix + key) flatMap {
////      case Some(variable) =>
////        state.expunge(variable.asInstanceOf[Option[Variable]].get) map {
////          case Some(b) => b
////          case None => throw new StorageException(s"Failed to expunge $key")
////        }
////      case None => throw new StorageException(s"Failed to read $key")
////    }
////  }
//
//  def get(key: String): Option[T] = ???
//
////  override def +[B1 >: T](kv: (String, B1)): MesosStore[T] = {
////    case(key, value) =>
////      state.fetch(prefix + key).flatMap{
////
////
//
////        case Some(variable) =>
////              this
////        case None =>
////          throw new StorageException(s"Failed to store $key")
////      }
////    this
////  }
//
//
////  override def -=(key: String): mesosphere.cassandra.MesosStore[T] = this
////
////  override def +=(kv: (String, T)): mesosphere.cassandra.MesosStore[T] = this
//
//
//}