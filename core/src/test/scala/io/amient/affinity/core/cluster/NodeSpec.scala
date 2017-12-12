package io.amient.affinity.core.cluster

import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.actor.{Partition, Service}
import io.amient.affinity.core.config.Cfg
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class NodeSpec extends FlatSpec with Matchers {

  behavior of "Node.Config"

  it should "report missing properties only if they are required" in {
    val caught = intercept[IllegalArgumentException] {
      Cfg.apply(ConfigFactory.empty(), Node.Config)
    }
    caught.getMessage should be(
      "shutdown.timeout.ms is required in affinity.node\n" +
        "startup.timeout.ms is required in affinity.node\n")
  }
  it should "report invalid property types whether optional or required" in {
    val caught = intercept[IllegalArgumentException] {
      val config = ConfigFactory.parseMap(Map(
        Node.Config.StartupTimeoutMs.path -> "hello",
        Node.Config.ShutdownTimeoutMs.path -> "hello",
        Node.Config.Services.path() -> "hello"
      ))
      Cfg.apply(config, Node.Config)
    }
    caught.getMessage should be(
      "hardcoded value: service has type STRING rather than OBJECT\n" +
        "shutdown.timeout.ms must be of type Long\n" +
        "startup.timeout.ms must be of type Long\n")
  }

  it should "report invalid property for different struct in a group" in {
    val caught = intercept[IllegalArgumentException] {
      val config = ConfigFactory.parseMap(Map(
        Node.Config.StartupTimeoutMs.path -> 100L,
        Node.Config.ShutdownTimeoutMs.path -> 1000L,
        Node.Config.Services.path("wrongstruct") -> Map(
          "something.we.dont.recognize" -> 20
        ).asJava
      ))
      Cfg.apply(config, Node.Config)
    }
    caught.getMessage should be("something.we.dont.recognize is not a known property of affinity.node.service\n" +
      "class is required in affinity.node.service\n")
  }

  it should "report recoginzie correct group type" in {
    val caught = intercept[IllegalArgumentException] {
      val config = ConfigFactory.parseMap(Map(
        Node.Config.StartupTimeoutMs.path -> 100L,
        Node.Config.ShutdownTimeoutMs.path -> 1000L,
        Node.Config.Services.path("wrongclass") -> Map(
          Service.Config.PartitionClass.path -> classOf[Node].getName
        ).asJava
      ))
      Cfg.apply(config, Node.Config)
    }
    caught.getMessage should be("io.amient.affinity.core.cluster.Node is not an instance of io.amient.affinity.core.actor.Partition\n")
  }

  it should "recognize correctly configured not" in {
    val config = ConfigFactory.parseMap(Map(
      Node.Config.StartupTimeoutMs.path -> 100L,
      Node.Config.ShutdownTimeoutMs.path -> 1000L,
      Node.Config.Services.path("myservice") -> Map(
        Service.Config.PartitionClass.path -> classOf[Partition].getName
      ).asJava
    ))
    Cfg.apply(config, Node.Config).Services("myservice").PartitionClass.get() should be (classOf[Partition])
  }

}
