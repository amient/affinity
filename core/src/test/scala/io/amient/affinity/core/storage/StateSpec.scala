package io.amient.affinity.core.storage

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.AvroRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._

case class ExpirableValue(data: String, val eventTimeUtc: Long) extends AvroRecord[ExpirableValue] with EventTime

class StateSpec extends TestKit(ActorSystem.create("test",
  ConfigFactory.parseMap(Map(
    State.CONFIG_STATE_STORE("test-state-1") -> Map(
      State.CONFIG_MEMSTORE_CLASS -> classOf[MemStoreSimpleMap].getName,
      State.CONFIG_STORAGE_CLASS -> classOf[NoopStorage].getName
    ).asJava,
    State.CONFIG_STATE_STORE("test-state-2") -> Map(
      State.CONFIG_MEMSTORE_CLASS -> classOf[MemStoreSimpleMap].getName,
      State.CONFIG_STORAGE_CLASS -> classOf[NoopStorage].getName,
      State.CONFIG_TTL_SECONDS -> 5
    ).asJava
  ).asJava).withFallback(ConfigFactory.defaultReference())))
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  behavior of "State"

  it should "work without ttl" in {
    implicit val partition = 0
    val state = new State[Long, ExpirableValue]("test-state-1", system)

    val nowMs = System.currentTimeMillis()

    state.insert(1L, ExpirableValue("one", nowMs - 9000))
    state.insert(2L, ExpirableValue("two", nowMs - 3000))
    state.insert(3L, ExpirableValue("three", nowMs))
    state(1L) should be (Some(ExpirableValue("one", nowMs - 9000)))
    state(2L) should be (Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be (Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be (3L)
  }

  it should "clean expired entries when ttl set" in {

    implicit val partition = 0
    val state = new State[Long, ExpirableValue]("test-state-2", system)

    val nowMs = System.currentTimeMillis()

    state.insert(1L, ExpirableValue("one", nowMs - 9000))
    state.insert(2L, ExpirableValue("two", nowMs - 3000))
    state.insert(3L, ExpirableValue("three", nowMs))
    state(1L) should be (None)
    state(2L) should be (Some(ExpirableValue("two", nowMs - 3000)))
    state(3L) should be (Some(ExpirableValue("three", nowMs)))
    state.iterator.size should be (2L)
  }


}

