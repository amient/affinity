package io.amient.affinity.stream

import java.util
import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class BinaryStreamImpl(config: Config, topic: String) extends BinaryStream {

  private val log = LoggerFactory.getLogger(classOf[BinaryStreamImpl])

  val producerConfig = new Properties() {
    config.entrySet().foreach { case entry => put(entry.getKey, entry.getValue.unwrapped()) }
    config.getConfig("producer").entrySet().foreach { case entry => put(entry.getKey, entry.getValue.unwrapped()) }
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
  }

  private val consumerProps = new Properties() {
    require(config != null)
    put("auto.offset.reset", "earliest")
    config.entrySet().foreach { case entry => put(entry.getKey, entry.getValue.unwrapped()) }
    config.getConfig("consumer").entrySet().foreach { case entry => put(entry.getKey, entry.getValue.unwrapped()) }
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  private val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  private val partitionProgress = new mutable.HashMap[TopicPartition, Long]()
  private var closed = false
  private var snapshot: collection.Map[TopicPartition, (Long, Long)] = null

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  private def takeSnapshot(tps: Seq[TopicPartition]) = {
    val startOffsets = kafkaConsumer.beginningOffsets(tps)
    snapshot = kafkaConsumer.endOffsets(tps).map {
      case (tp, endOffset) => (tp, (startOffsets(tp).toLong, endOffset.toLong))
    }
  }

  override def subscribe(): Unit = {
    val tps = (0 until getNumPartitions()).map(p => new TopicPartition(topic, p))
    takeSnapshot(tps)
    kafkaConsumer.subscribe(List(topic))
  }

  override def subscribe(partition: Int): Unit = {
    val tps = List(new TopicPartition(topic, partition))
    takeSnapshot(tps)
    kafkaConsumer.assign(tps)
  }

  override def lag(): Long = {
    snapshot.map {
      case (tp, (_, endOffset)) if partitionProgress.contains(tp) => endOffset - partitionProgress(tp) - 1
      case (_, (startOffset, endOffset)) => endOffset - startOffset
    }.max
  }

  override def fetch(minTimestamp: Long): util.Iterator[Record[Array[Byte], Array[Byte]]] = {
    val kafkaRecords = kafkaConsumer.poll(6000).iterator()
    var fastForwarded = false
    val filteredKafkaRecords = kafkaRecords.filter {
      record =>
        val isAfterMinTimestamp = record.timestamp() >= minTimestamp
        val tp = new TopicPartition(record.topic, record.partition)
        if (!partitionProgress.contains(tp) || record.offset > partitionProgress(tp)) {
          partitionProgress.put(tp, record.offset)
        }
        if (!isAfterMinTimestamp && !fastForwarded) {
          kafkaConsumer.offsetsForTimes(Map(new TopicPartition(record.topic(), record.partition()) -> new java.lang.Long(minTimestamp))).foreach {
            case (tp, oat) => if (oat.offset > record.offset) {
              log.info(s"Fast forward partition ${record.topic()}/${record.partition()} because record.timestamp(${record.timestamp()}) < $minTimestamp")
              kafkaConsumer.seek(tp, oat.offset())
            }
          }
          fastForwarded = true
        }
        isAfterMinTimestamp
    }

    filteredKafkaRecords.map {
      case r => new Record(r.key(), r.value(), r.timestamp())
    }
  }

  def commit() = {
    kafkaConsumer.commitAsync()
  }

  def active() = !closed


  override def publish(iter: util.Iterator[PartitionedRecord[Array[Byte], Array[Byte]]]): Long = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)
    try {
      var messages = 0L
      while (iter.hasNext) {
        val kpr = iter.next()
        producer.send(new ProducerRecord(topic, kpr.partition, kpr.record.key, kpr.record.value))
        messages += 1
      }
      messages
    } finally {
      producer.close()
    }
  }

  override def close(): Unit = {
    try kafkaConsumer.close() finally closed = true
  }

}

