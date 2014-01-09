package mesosphere.cassandra

import scala.collection.mutable

/**

 */
@Deprecated
object KVStoreMap extends KVStore{

  val store = mutable.HashMap[String, Any]()

  def put(key :String, value: Any) = store.put(key, value)

  def get(key :String) =  store.get(key)

  def contains(key: String) = store.contains(key)

  def getOrElse(key :String, defaultVal :Any) = { getOrElse(key, defaultVal) }

}

trait KVStore{

  def put(key :String, value: Any)

  def get(key :String) :Any

  def contains(key :String) : Boolean

  def getOrElse(key :String, defaultVal :Any) :Any
}
