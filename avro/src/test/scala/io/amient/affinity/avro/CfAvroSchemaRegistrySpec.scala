package io.amient.affinity.avro

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.kafka.EmbeddedCfRegistry
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

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

    val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
    serde.register[Record](v1schema)

    serde.register[Record]
    serde.initialize()

    val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
    serde.register[Record](v3schema)
    val thrown = intercept[RuntimeException](serde.initialize())
    thrown.getMessage should startWith("Schema being registered is incompatible with an earlier schema")

  }
}
