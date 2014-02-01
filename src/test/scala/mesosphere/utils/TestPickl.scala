package mesosphere.utils

import scala.pickling._
import json._
import scala.collection.mutable
import reflect.runtime.universe._


object TestPickl extends App{


  class Oink[S](){

    val store = mutable.Map[String, String]()

    def fetch[S](key :String) = { store.get(key).get.unpickle[Any]  }

    def store(key :String, value:S) :S= {
      store.put(key, value.pickle.value)
      value
    }

  }


  val o =new Oink[String]()

  o.store("key", "value")

  println("value: " + o.fetch[String]("key"))


}
