package io.amient.affinity.core.cluster

import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.config.Cfg
import org.scalatest.{FlatSpec, Matchers}

class NodeSpec extends FlatSpec with Matchers {

  behavior of "Node.Config"

  it should "report missing properties only if they are required" in {
    val caught = intercept[IllegalArgumentException] {
      Cfg.apply(ConfigFactory.empty(), Node.Config).ConfigStartupTimeoutMs.get()
    }
    println(caught.getMessage)
    caught.getMessage should be(
      "shutdown.timeout.ms is required in affinity.node\n" +
        "startup.timeout.ms is required in affinity.node\n")
  }
  it should "report missing properties and invalid property types" in {
    val caught = intercept[IllegalArgumentException] {
      Cfg.apply(ConfigFactory.empty(), Node.Config).ConfigStartupTimeoutMs.get()
    }
    caught.getMessage should be(
      "service is required in affinity.node\n" +
        "shutdown.timeout.ms is required in affinity.node\n" +
        "startup.timeout.ms is required in affinity.node\n")
  }
}
