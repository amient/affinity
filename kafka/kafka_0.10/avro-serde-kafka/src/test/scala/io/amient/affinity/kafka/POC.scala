package io.amient.affinity.kafka

import java.util.UUID

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.{AvroRecord, schema}
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

object POC extends App {
  final val topic = "transactions"

  val producerConfig = Map(
    "bootstrap.servers" -> "localhost:9092",
    "zookeeper.connect" -> "localhost:2181",
    "key.serializer" -> "io.amient.affinity.kafka.KafkaAvroSerializer",
    "value.serializer" -> "io.amient.affinity.kafka.KafkaAvroSerializer",
    "affinity.avro.schema.provider.class" -> "io.amient.affinity.avro.schema.CfAvroSchemaRegistry",
    "affinity.avro.confluent-schema-registry.url.base" -> "http://localhost:8082"
  )
  println("registering producer")
  val producer = new KafkaProducer[String, Transaction](producerConfig)

  println("producing record")
  val sent = producer.send(generateTransaction)
  println("acking record")
  sent.get()
  println("closing producer")
  producer.close()

  val consumerConfig = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "affinity-consumer",
    "key.deserializer" -> "io.amient.affinity.kafka.KafkaAvroDeserializer",
    "value.deserializer" -> "io.amient.affinity.kafka.KafkaAvroDeserializer",
    "affinity.avro.schema.provider.class" -> "io.amient.affinity.avro.schema.CfAvroSchemaRegistry",
    "affinity.avro.confluent-schema-registry.url.base" -> "http://localhost:8082"
  )
  val consumerSerde = new CfAvroSchemaRegistry(ConfigFactory
    .empty.withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef("http://localhost:8082")))

  println("registering consumer")
  val consumer = new KafkaConsumer[String, Transaction](consumerConfig)
  consumer.subscribe(List(topic))

  println("consuming")
  consumer.poll(15000).foreach {
    record: ConsumerRecord[String, Transaction] =>
      println(record.value())
  }
  println("closing consumer")
  consumer.close()


  def generateTransaction: ProducerRecord[String, Transaction] = {
    val txid = UUID.randomUUID().toString
    new ProducerRecord(
      topic,
      txid,
        Transaction(
          txid,
          Some("customerExternalId"),
          "storeId",
          "tillId",
          System.currentTimeMillis() / 1000 - 10,
          List(BasketItem(
            "ean",
            None,
            Some(1000), //actualPrice
            Some(1000), //regularPrice
            1000, //totalPrice
            Some(1), //quantity
            Some(10), //weightGrams
            Some(100) //volumeMl
          )), // basket
          List(), //cancelled
          0,
          1000,
          1000,
          0
        ))
  }
}

case class Transaction(transactionId: String,
                       loyaltyId: Option[String],
                       storeId: String,
                       tillId: String,
                       timestamp: Long,
                       basket: Iterable[BasketItem],
                       cancelledItems: Iterable[BasketItem],
                       loyaltyPointsSpent: Int,
                       totalPrice: Int,
                       totalNet: Int,
                       totalDiscount: Int) extends AvroRecord[Transaction]

object Sensitiveness extends Enumeration {
  type Sensitiveness = Value
  val REGULAR, SENSITIVE = Value
}

case class BasketItem(ean: String,
                      sku: Option[String],
                      actualPrice: Option[Int],
                      regularPrice: Option[Int],
                      totalPrice: Int,
                      quantity: Option[Int],
                      weightGrams: Option[Int],
                      volumeMl: Option[Int],
                      sensitiveness: Sensitiveness.Value = Sensitiveness.REGULAR ) extends AvroRecord[BasketItem]

object BasketItem {

  def subtractItems(positive: Iterable[BasketItem], negative: Iterable[BasketItem]): Iterable[BasketItem] =
    positive.filter(positiveItem =>
      negative.forall(cancelledItem =>
        // this is not a full comparison because cancelled items are listed differently in R10
        positiveItem.ean != cancelledItem.ean ||
          positiveItem.sku != cancelledItem.sku ||
          positiveItem.totalPrice != cancelledItem.totalPrice
      ))
}