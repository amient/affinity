package io.amient.affinity.spark

import java.time.temporal.TemporalAmount
import java.time.{Duration, Instant, Period}

import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.actor.Routed
import io.amient.affinity.core.storage.Storage
import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.{EventTime, TimeRange}
import io.amient.affinity.kafka.EmbeddedKafka
import io.amient.affinity.spark.CompactRDDTestUniverse.{getSerdeConf, getStorageConf}
import io.amient.affinity.stream.{BinaryRecord, BinaryStream}
import io.amient.util.spark.CompactRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

case class CompactionTestEvent(key: Int, data: String, ts: Long) extends AvroRecord with EventTime with Routed {
  override def eventTimeUnix() = ts
}

object CompactRDDTestUniverse {

  val topic = "test-topic"
  val DecemberFirst2017 = Instant.ofEpochMilli(1512086401000L)
  val JanuaryFirst2018 = Instant.ofEpochMilli(1514764801000L)
  val FebruaryFirst2018 = Instant.ofEpochMilli(1517443201000L)
  val schemaRegistryId = "345"

  def getSerdeConf = new AvroConf().apply(Map(
    AvroSerde.Conf.Class.path -> classOf[MemorySchemaRegistry].getName,
    MemorySchemaRegistry.Conf.ID.path -> schemaRegistryId
  ))

  def getStorageConf(kafkaBootstrap: String) = new StorageConf().apply(Map(
    Storage.Conf.Class.path -> classOf[KafkaStorage].getName,
    KafkaStorage.Conf.BootstrapServers.path -> kafkaBootstrap,
    KafkaStorage.Conf.Topic.path -> topic
  ))

  def avroCompactRdd[K: ClassTag, V: ClassTag](avroConf: AvroConf, storageConf: StorageConf, range: TimeRange = TimeRange.ALLTIME)
                                              (implicit sc: SparkContext): RDD[(K, V)] = {
    CompactRDD(AvroSerde.create(avroConf), BinaryStream.bindNewInstance(storageConf), range)
  }

}

class StreamOutput(kafkaBootstrap: String) {
  lazy val stream: BinaryStream = BinaryStream.bindNewInstance(getStorageConf(kafkaBootstrap))
  lazy val serde = AvroSerde.create(getSerdeConf)
  private var active = false
  def output(data: Iterator[Any]): Unit = {
    active = true
    stream.publish(data.map {
      case event:Routed with EventTime => new BinaryRecord(serde.toBytes(event.key), serde.toBytes(event), event.eventTimeUnix)
      case event:EventTime => new BinaryRecord(null, serde.toBytes(event), event.eventTimeUnix)
      case event:Routed => new BinaryRecord(serde.toBytes(event.key), serde.toBytes(event), EventTime.unix)
      case event:Any => new BinaryRecord(null, serde.toBytes(event), EventTime.unix)
    })
  }

  def close() = if (active) {
    try serde.close() finally try stream.flush() finally stream.close()
  }
}

class CompactRDDTest extends FlatSpec with EmbeddedKafka with Matchers with BeforeAndAfterAll {

  override def numPartitions = 10

  import CompactRDDTestUniverse._

  implicit val sc = new SparkContext(new SparkConf()
    .setMaster("local[10]")
    .set("spark.driver.host", "localhost")
    .setAppName("Affinity_Spark_Test")
    .set("spark.serializer", classOf[KryoSerializer].getName))

  override def beforeAll() {
    super.beforeAll()


    val stream = new StreamOutput(kafkaBootstrap)
    try {
      stream.output((0 to 99).iterator.map { i =>
        CompactionTestEvent(i, s"January($i)", JanuaryFirst2018.toEpochMilli + i * 1000)
      })

      stream.output((0 to 99).iterator.map { i =>
        CompactionTestEvent(i, s"February($i)", FebruaryFirst2018.toEpochMilli + i * 1000)
      })

      stream.output((0 to 99).iterator.map { i =>
        CompactionTestEvent(i, s"December($i)", DecemberFirst2017.toEpochMilli + i * 1000)
      })

    } finally {

      stream.close
    }
  }

  "full scan RDD" should "return fully compacted stream" in {
    val rdd = avroCompactRdd[Int, CompactionTestEvent](getSerdeConf, getStorageConf(kafkaBootstrap))
    val result = rdd.collect.sortBy(_._1)
    result.size should be (100)
    result.forall(_._2.eventTimeUnix >= FebruaryFirst2018.toEpochMilli)
  }

  "range scan RDD" should "return compacted range of the stream" in {
    val rdd = avroCompactRdd[Int, CompactionTestEvent](
      getSerdeConf, getStorageConf(kafkaBootstrap), TimeRange.prev(Duration.ofSeconds(50), Instant.from(Duration.ofSeconds(100).addTo(FebruaryFirst2018))))
    val result = rdd.collect.sortBy(_._1)
    result.size should be (50)
    result.forall(_._2.eventTimeUnix >= FebruaryFirst2018.toEpochMilli)
  }

}
