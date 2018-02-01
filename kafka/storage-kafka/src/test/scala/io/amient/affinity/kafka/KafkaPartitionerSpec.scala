package io.amient.affinity.kafka

import java.util

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.Murmur2Partitioner
import org.apache.kafka.common.{Cluster, Node, PartitionInfo}
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class KafkaPartitionerSpec extends FlatSpec with Matchers {

  def mockCluster(numParts: Int) = new Cluster("mock-cluster",
    util.Arrays.asList[Node](),
    (0 to numParts - 1).map(p => new PartitionInfo("test", p, null, Array(), Array())),
    new util.HashSet[String],
    new util.HashSet[String])

  "kafka.DefaultPartitioner" should "have identical method to Murmur2Partitioner" in {
    val kafkaPartitioner = new org.apache.kafka.clients.producer.internals.DefaultPartitioner()
    val affinityPartitioner = new Murmur2Partitioner
    val key = "test-value-for-partitioner"
    val serializedKey: Array[Byte] = key.getBytes
    val kafkaP = kafkaPartitioner.partition("test", key, serializedKey, key, serializedKey, mockCluster(4))
    val affinityP = affinityPartitioner.partition(serializedKey, 4)
    kafkaP should equal(affinityP)
  }

  "KafkaAvroSerde" should "have identical serialization footprint as Akka AvroSerdeProxy" in {
    val cfg = Map(
      "schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
      "schema.registry.id" -> "1"
    )

    val key = "6290853012217500191217"

    val system = ActorSystem.create("test",
      ConfigFactory.parseMap(cfg.map { case (k, v) => ("affinity.avro." + k, v) })
        .withFallback(ConfigFactory.defaultReference()))
    val serialization = SerializationExtension(system)
    val akkaSerializedKey = serialization.serialize(key).get
    system.terminate()
    val kafkaSerde = new KafkaAvroSerde()
    kafkaSerde.configure(cfg, true)
    val kafkaSerialized = kafkaSerde.serializer().serialize("test", key)
    akkaSerializedKey.mkString(".") should equal(kafkaSerialized.mkString("."))
    new Murmur2Partitioner().partition(akkaSerializedKey, 9) should be(4)
    new Murmur2Partitioner().partition(kafkaSerialized, 9) should be(4)
    val streamsPartitioner = new DefaultStreamPartitioner[Any, Any](kafkaSerde.serializer(), mockCluster(9), "test")
    streamsPartitioner.partition(key, null, 9) should be(4)
    streamsPartitioner.partition(key, "irrelevant", 9) should be(4)
  }

}
