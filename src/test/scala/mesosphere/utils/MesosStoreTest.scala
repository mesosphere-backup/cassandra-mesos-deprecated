package mesosphere.utils

//import org.specs2.mutable._

import org.apache.mesos.state.{ZooKeeperState, State}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._
import java.util.concurrent.TimeUnit


@RunWith(classOf[JUnitRunner])
class MesosStoreTest extends FunSuite {

  var state: State = _
  var store: StateStore = _
  val time = System.currentTimeMillis()

  state = new ZooKeeperState(
    "localhost:2181",
    5000,
    TimeUnit.MILLISECONDS,
    "/cassMesosStateTest"
  )

  store = new StateStore(state)

  test("Persisting") {
    store.store("testKey2", "oink-" + time)
  }

  test("Retrieving existing key") {
    assert(Some("oink-" + time) == store.fetch[String]("testKey2"))
  }

  test("Retrieving missing key") {
    assert(None == store.fetch("non-existent"))
  }

  test("Expunging existing key") {
    assert(None == store.fetch[Long]("testKey2" + time))
    store.store("testKey2" + time, time)
    assert(Some(time) == store.fetch[Long]("testKey2" + time))
    assert(true == store.expunge("testKey2" + time))
    assert(None == store.fetch[Long]("testKey2" + time))
  }

  test("Expunging missing key") {
    assert(false == store.expunge("thisDoesNotExist" + System.currentTimeMillis()))
  }

  test("Iterating through keys") {
    val SIZE = 10

    // remove what's there to cleanup
    store.names().foreach {
      k =>
      assert(true == store.expunge(k))
    }

    // create new entries
    (0 until SIZE).foreach {
      i =>
        store.store(i.toString, i)
    }

    // see if we got it all back
    val fetched = store.names().toSeq
    assert(SIZE == fetched.length)
    (0 until SIZE).foreach{
      i =>
      assert(true == fetched.contains(i.toString))
    }

  }

}
