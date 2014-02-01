package mesosphere.utils

//import org.specs2.mutable._

import org.apache.mesos.state.{ZooKeeperState, State}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._
import java.util.concurrent.TimeUnit
import scala.pickling._
import json._


@RunWith(classOf[JUnitRunner])
class ExampleSpec extends FunSuite {

  var state: State = _
  var store: MesosStore[String] = _

  def bytesToHex(bytes : Array[Byte]) =
    bytes.map{ b => String.format("%02X", java.lang.Byte.valueOf(b)) }.mkString(" ")

  state = new ZooKeeperState(
    "localhost:2181",
    5000,
    TimeUnit.MILLISECONDS,
    "/cassMesosStateTest"
  )

  store = new MesosStore[String](state)

  val time = System.currentTimeMillis()

  val pckl = List(1, 2, 3, 4).pickle
  println("tttttttt " + pckl)
  val lst = pckl.unpickle[List[Int]]

  println("sss " + lst)

  assert(None == store.fetch("non-existant"))


  //  test("Serialization & Deserialization"){
//    val helloSer = store.objToBytes("oink2".asInstanceOf[Serializable])
//    println("serialized: " + bytesToHex(helloSer))
//    val helloDes = store.bytesToObj(helloSer)
//    println("deserialized: " + bytesToHex(helloDes))
//  }

  test("Persisting") {
    store.store("testKey2", "oink-" + time )
  }

  test("Retrieving") {
    println("xxxxxx " + store.fetch("testKey2"))
    assert(Some("oink-" + time) == store.fetch("testKey2"))
  }
}

