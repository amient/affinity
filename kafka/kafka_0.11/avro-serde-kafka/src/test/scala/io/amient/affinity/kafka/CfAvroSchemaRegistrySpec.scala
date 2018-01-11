package io.amient.affinity.kafka

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.AvroRecord
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry.CfAvroConf
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

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

case class CompositeRecord(
                   val items: Seq[SimpleRecord] = Seq(),
                   val index: Map[String, SimpleRecord] = Map(),
                   val setOfPrimitives: Set[Long] = Set() ) extends AvroRecord


class CfAvroSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedCfRegistry {

  override def numPartitions = 1

  behavior of "CfAvroSchemaRegistry"

  val serde = new CfAvroSchemaRegistry(ConfigFactory.parseMap(Map(
    new CfAvroConf().ConfluentSchemaRegistryUrl.path -> registryUrl
  )))

  serde.register[SimpleKey]
  serde.register[SimpleRecord]
  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  serde.register[CompositeRecord](v1schema)
  serde.initialize()

  it should "allow compatible version of previously registered schema" in {
    serde.register[CompositeRecord]
    serde.initialize() should be(List(11))
  }

  it should "reject incompatible schema registration" in {

    val thrown = intercept[RuntimeException]{
      val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
      serde.register[CompositeRecord](v3schema)
      serde.initialize()
    }
    thrown.getMessage should include("incompatible")

  }

  it should "registerd topic subject when fqn subject is already registered" in {
    val data = SimpleRecord()
    //fqn should be already registered
    serde.getCurrentSchema(classOf[SimpleRecord].getName) should be(Some((9, data.getSchema)))
    //now simulate what KafkaAvroSerde would do
    val (objSchema, schemaId) = try {
      serde.getOrRegisterSchema(data, "topic-simple")
    } catch {
      case e: RuntimeException =>
        e.printStackTrace()
        fail("could not register schema with topic-simple")
    }
    schemaId should be(9)
    objSchema should be(data.getSchema)
    //and check the additional subject was registered with the same schema
    val versions = serde.getVersions("topic-simple")
    versions should be (Some(Map(9 -> data.getSchema)))
  }
}
