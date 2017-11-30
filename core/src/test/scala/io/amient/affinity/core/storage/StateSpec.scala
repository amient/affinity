package io.amient.affinity.core.storage

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.AvroRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

case class ExpirableValue(data: String, val eventTimeUtc: Long) extends AvroRecord[ExpirableValue] with EventTime

class StateSpec extends TestKit(ActorSystem.create("test", ConfigFactory.defaultReference()))
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  behavior of "State"

  it should "work without ttl" in {
    val stateConfig = ConfigFactory.empty()
      .withValue(State.CONFIG_MEMSTORE_CLASS, ConfigValueFactory.fromAnyRef(classOf[MemStoreSimpleMap].getName))
      .withValue(State.CONFIG_STORAGE_CLASS, ConfigValueFactory.fromAnyRef(classOf[NoopStorage].getName))

    implicit val partition = 0
    val state = new State[Long, ExpirableValue]("test-state-1", system, stateConfig)

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

    val stateConfig = ConfigFactory.empty()
        .withValue(State.CONFIG_MEMSTORE_CLASS, ConfigValueFactory.fromAnyRef(classOf[MemStoreSimpleMap].getName))
        .withValue(State.CONFIG_STORAGE_CLASS, ConfigValueFactory.fromAnyRef(classOf[NoopStorage].getName))
        .withValue(State.CONFIG_TTL_SECONDS, ConfigValueFactory.fromAnyRef(5))

    implicit val partition = 0
    val state = new State[Long, ExpirableValue]("test-state-2", system, stateConfig)

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

