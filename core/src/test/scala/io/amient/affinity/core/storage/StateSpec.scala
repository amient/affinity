package io.amient.affinity.core.storage

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.EventTime
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class ExpirableValue(data: String, val eventTimeUnix: Long) extends AvroRecord with EventTime

class StateSpec extends TestKit(ActorSystem.create("test",
  ConfigFactory.parseMap(Map(
    Node.Conf.Affi.Avro.Class.path -> classOf[MemorySchemaRegistry].getName,
    Node.Conf.Affi.Gateway.Http.Host.path -> "127.0.0.1",
    Node.Conf.Affi.Gateway.Http.Port.path -> "0"
  )).withFallback(ConfigFactory.defaultReference)))
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  val specTimeout = 5 seconds

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  behavior of "State"

  it should "not allow writes and deletes in read-only state" in {
    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      State.StateConf.Storage.Class.path -> classOf[NoopStorage].getName,
      State.StateConf.External.path -> "true"
    )))
    val state = State.create[Long, ExpirableValue]("read-only-store", 0, stateConf, 1, system)
    an[RuntimeException] should be thrownBy (Await.result(state.insert(1L, ExpirableValue("one", 1)), specTimeout))
    an[RuntimeException] should be thrownBy (Await.result(state.delete(1L), specTimeout))
  }

  it should "work without ttl" in {

    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      State.StateConf.Storage.Class.path -> classOf[NoopStorage].getName
    )))
    val state = State.create[Long, ExpirableValue]("no-ttl-store", 0, stateConf, 1, system)

    val nowMs = System.currentTimeMillis()

    Await.result(Future.sequence(List(
      state.insert(1L, ExpirableValue("one", nowMs - 9000)),
      state.insert(2L, ExpirableValue("two", nowMs - 3000)),
      state.insert(3L, ExpirableValue("three", nowMs)))), specTimeout)
    state(1L) should be(Some(ExpirableValue("one", nowMs - 9000)))
    state(2L) should be(Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be(Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be(3L)
  }

  it should "clean expired entries when ttl set" in {
    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      State.StateConf.Storage.Class.path -> classOf[NoopStorage].getName,
      State.StateConf.TtlSeconds.path -> 5
    )))

    val state = State.create[Long, ExpirableValue]("ttl-store", 0, stateConf, 1, system)

    val nowMs = System.currentTimeMillis()

    Await.result(Future.sequence(List(
      state.insert(1L, ExpirableValue("one", nowMs - 9000)),
      state.insert(2L, ExpirableValue("two", nowMs - 3000)),
      state.insert(3L, ExpirableValue("three", nowMs)))), specTimeout)
    state(1L) should be(None)
    state(2L) should be(Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be(Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be(2L)
    state.numKeys should be(2L)
  }


}

