package io.amient.affinity.avro

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.kafka.EmbeddedZooKeeper
import org.apache.avro.{Schema, SchemaValidationException}
import org.scalatest.{FlatSpec, Matchers}

class ZookeeperSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedZooKeeper {

  behavior of "ZkAvroRegistry"

  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_Current\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_Current\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"index\",\"type\":{\"type\":\"map\",\"values\":\"SimpleRecord\"},\"default\":{}}]}")

  val serde = new ZookeeperSchemaRegistry(ConfigFactory.empty().withValue(
    new ZkAvroConf().Connect.path, ConfigValueFactory.fromAnyRef(zkConnect)
  ))
  serde.register[SimpleKey]
  serde.register[SimpleRecord]
  val backwardSchemaId = serde.register[Record_Current](v1schema)
  val currentSchemaId = serde.register[Record_Current]
  val forwardSchemaId = serde.register[Record_Current](v3schema)

  it should "work in a backward-compatibility scenario" in {
    val oldValue = Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), 10)
    val oldBytes = serde.write(oldValue, v1schema, backwardSchemaId)
    oldBytes.mkString(",") should be("0,0,0,0,10,2,2,4,0,0,20")
    val upgraded = serde.fromBytes(oldBytes)
    upgraded should be(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), Map()))
  }

  it should "work in a forward-compatibility scenario" in {
    val forwardValue = Record_V3(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)))
    val forwardBytes = serde.write(forwardValue, v3schema, forwardSchemaId)
    val downgraded = serde.fromBytes(forwardBytes)
    downgraded should be(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)), Set()))
  }

  it should "reject incompatible schema registration" in {
    val v4schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
    an[SchemaValidationException] should be thrownBy {
      serde.register[Record_Current](v4schema)
    }
  }

}
