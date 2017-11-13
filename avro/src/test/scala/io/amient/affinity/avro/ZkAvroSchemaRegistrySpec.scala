package io.amient.affinity.avro

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.avro.util.ZooKeeperClient
import io.amient.affinity.kafka.EmbeddedZooKeeper
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class ZkAvroSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedZooKeeper {
  //val v1schema = AvroRecord.inferSchema(classOf[Record_V1]); println(v1schema)
  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  //val v3schema = AvroRecord.inferSchema(classOf[Record_V3]); println(v3schema)
  val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"index\",\"type\":{\"type\":\"map\",\"values\":\"SimpleRecord\"},\"default\":{}}]}")

  val client = new ZooKeeperClient(zkConnect)
  val serde = new ZkAvroSchemaRegistry(ConfigFactory.defaultReference.withValue(
    ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT, ConfigValueFactory.fromAnyRef(zkConnect)
  ))
  serde.register(classOf[SimpleKey])
  serde.register(classOf[SimpleRecord])
  serde.register(classOf[Record], v1schema)
  serde.register(classOf[Record])
  serde.register(classOf[Record], v3schema)

  val List(_, _, _, _, _, _, _, _, _, backwardSchemaId, currentSchemaId, forwardSchemaId) = serde.initialize()

  "ZkAvroRegistry" should "work in a backward-compatibility scenario" in {
    val oldValue = Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), 10)
    val oldBytes = AvroRecord.write(oldValue, v1schema, backwardSchemaId)
    val upgraded = serde.fromBytes(oldBytes)
    upgraded should be(Record(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), Map()))
  }

  "ZkAvroRegistry" should "work in a forward-compatibility scenario" in {
    val forwardValue = Record_V3(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)))
    val forwardBytes = AvroRecord.write(forwardValue, v3schema, forwardSchemaId)
    val downgraded = serde.fromBytes(forwardBytes)
    downgraded should be(Record(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)), Set()))
  }

}
