package io.amient.affinity.example

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ExampleExternalStateSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers with BeforeAndAfterAll
  with TimeLimitedTests {

  override def numPartitions = 2

  val config = configure(ConfigFactory.load("example-external-state"))

  val topic = config.getString("affinity.keyspace.external.state.news.storage.kafka.topic")

  val node = new TestGatewayNode(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  override def beforeAll: Unit = node.awaitClusterReady()

  override def afterAll: Unit = node.shutdown()

  behavior of "External State"

  val timeLimit = Span(5000, Millis) //it should be much faster but sometimes many tests are run at the same time

  it should "start automatically tailing state partitions on startup even when master" in {

    val externalProducer = createKafkaAvroProducer[String, String]()
    val highestOffsets = try {
      val produced = List(
        externalProducer.send(new ProducerRecord(topic, "10:30", "the universe is expanding")),
        externalProducer.send(new ProducerRecord(topic, "11:00", "the universe is still expanding")),
        externalProducer.send(new ProducerRecord(topic, "11:30", "the universe briefly contracted but is expanding again")))
      externalProducer.flush()
      produced.map(_.get).map(m => (m.partition, m.offset)).groupBy(_._1).mapValues(_.map(_._2).max)
    } finally {
      externalProducer.close()
    }
    val watermark = (0 to numPartitions).map(p => highestOffsets.get(p).getOrElse(-1))
    //we don't need an arbitrary sleep to ensure the tailing state catches up with the writes above
    //before we fetch the latest news because the watermark is built into the request to make the test fast and deterministic
    val response = node.get_text(node.http_get(node.uri(s"/news/latest?w=${watermark.mkString(",")}")))
    response should be(
      "LATEST NEWS:\n" +
        "10:30\tthe universe is expanding\n" +
        "11:00\tthe universe is still expanding\n" +
        "11:30\tthe universe briefly contracted but is expanding again")


  }

  private def createKafkaAvroProducer[K, V]() = new KafkaProducer[K, V](new Properties {
    put("bootstrap.servers", kafkaBootstrap)
    put("acks", "1")
    put("key.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    put("value.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    //this simply adds all configs required by KafkaAvroSerializer
    config.getConfig("affinity.avro").entrySet().asScala.foreach { case (entry) =>
      put(entry.getKey, entry.getValue.unwrapped())
    }
  })


}
