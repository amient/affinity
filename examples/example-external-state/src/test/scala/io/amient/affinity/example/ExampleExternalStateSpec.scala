package io.amient.affinity.example

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ExampleExternalStateSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers with BeforeAndAfterAll {

  override def numPartitions = 2

  val config = configure(ConfigFactory.load("example-external-state"))

  val topic = config.getString("affinity.keyspace.external.state.news.storage.kafka.topic")

  val node = new TestGatewayNode(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  override def beforeAll: Unit = node.awaitClusterReady()

  override def afterAll: Unit = node.shutdown()

  behavior of "External State"

  it should "start automatically tailing state partitions on startup even when master" in {
    val externalProducer = createKafkaAvroProducer[String, String]()
    val highestAvailableOffsets = try {
      val produced = List(
        externalProducer.send(new ProducerRecord(topic, "10:30", "the universe is expanding")),
        externalProducer.send(new ProducerRecord(topic, "11:00", "the universe is still expanding")),
        externalProducer.send(new ProducerRecord(topic, "11:30", "the universe briefly contracted but is expanding again")))
      externalProducer.flush()
      produced.map(_.get).map(m => (m.partition, m.offset)).groupBy(_._1).mapValues(_.map(_._2).max)
    } finally {
      externalProducer.close()
    }
    println(highestAvailableOffsets)
    Thread.sleep(1000) // TODO #150 do something more clever with lastProducedOffset than simply waiting for a second

    val response = node.get_text(node.http_get(node.uri("/news/latest")))
    println(response)
    //TODO #150 this is the bug - the external state is not tailed on startup so it sees none of the news generated above
//    response should be(
//      "LATEST NEWS:\n" +
//        "10:30\tthe universe is expanding\n" +
//        "11:00\tthe universe is still expanding\n" +
//        "11:30\tthe universe briefly contracted but is expanding again")


  }

  private def createKafkaAvroProducer[K, V]() = new KafkaProducer[K, V](new Properties {
    put("bootstrap.servers", kafkaBootstrap)
    put("ack", "1")
    put("key.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    put("value.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    //this simply adds all configs required by KafkaAvroSerializer
    config.getConfig("affinity.avro").entrySet().asScala.foreach { case (entry) =>
      put(entry.getKey, entry.getValue.unwrapped())
    }
  })


}
