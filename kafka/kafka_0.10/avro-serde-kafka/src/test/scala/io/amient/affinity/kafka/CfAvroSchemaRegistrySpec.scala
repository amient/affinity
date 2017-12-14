package io.amient.affinity.kafka

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

object SimpleEnum extends Enumeration {
  type SimpleEnum = Value
  val A, B, C = Value
}

case class SimpleKey(val id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class SimpleRecord(val id: SimpleKey = SimpleKey(0), val side: SimpleEnum.Value = SimpleEnum.A, val seq: Seq[SimpleKey] = Seq()) extends AvroRecord{
  override def hashCode(): Int = id.hashCode()
}

case class Record(
                   val items: Seq[SimpleRecord] = Seq(),
                   val index: Map[String, SimpleRecord] = Map(),
                   val setOfPrimitives: Set[Long] = Set() ) extends AvroRecord


class CfAvroSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedCfRegistry {

  override def numPartitions = 1

  behavior of "CfAvroSchemaRegistry"

  it should "reject incompatible schema registration" in {

    val serde = new CfAvroSchemaRegistry(ConfigFactory.defaultReference.withValue(
      CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl)
    ))

    serde.register[SimpleKey]
    serde.register[SimpleRecord]
    serde.initialize()

    val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
    serde.register[Record](v1schema)
    serde.register[Record]
    serde.initialize()

    val thrown = intercept[RuntimeException]{
      val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
      serde.register[Record](v3schema)
      serde.initialize()
    }
    thrown.getMessage should include("incompatible")

  }
}
