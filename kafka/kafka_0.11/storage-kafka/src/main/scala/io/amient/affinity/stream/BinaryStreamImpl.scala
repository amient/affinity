package io.amient.affinity.stream

import java.util
import java.util.Properties

import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.kafka.KafkaStorage.KafkaStorageConf
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class BinaryStreamImpl(conf: StorageConf) extends BinaryStream {

  private val log = LoggerFactory.getLogger(classOf[BinaryStreamImpl])

  val kafkaStorageConf = KafkaStorageConf(conf)

  val topic = kafkaStorageConf.Topic()

  val producerConfig = new Properties() {
    if (kafkaStorageConf.BootstrapServers.isDefined()) put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    if (kafkaStorageConf.Producer.isDefined) {
      val producerConfig = kafkaStorageConf.Producer.config()
      if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom key.serializer")
      if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom value.serializer")
      producerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
  }

  private val consumerProps = new Properties() {
    put("auto.offset.reset", "earliest")
    if (kafkaStorageConf.BootstrapServers.isDefined()) put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    if (kafkaStorageConf.Consumer.isDefined) {
      val consumerConfig = kafkaStorageConf.Consumer.config()
      consumerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  private val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  private val partitionProgress = new mutable.HashMap[TopicPartition, Long]()
  private var closed = false
  private var progress: (Long, Long) = (0, -1)

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  override def subscribe(): Unit = {
    kafkaConsumer.subscribe(List(topic))
  }

  override def subscribe(partition: Int): Unit = {
    val tp = new TopicPartition(topic, partition)
    kafkaConsumer.assign(List(tp))
    val startOffset = kafkaConsumer.beginningOffsets(List(tp))(tp).toLong
    progress = kafkaConsumer.endOffsets(List(tp))(tp) match {
      case endOffset => (Math.max(kafkaConsumer.position(tp), startOffset) - 1, endOffset.toLong - 1)
    }
  }

  override def lag(): Long = {
    progress._2 - progress._1
  }

  override def fetch(minTimestamp: Long): util.Iterator[Record[Array[Byte], Array[Byte]]] = {
    val kafkaRecords = kafkaConsumer.poll(6000).iterator()
    var fastForwarded = false
    val filteredKafkaRecords = kafkaRecords.filter {
      record =>
        val isAfterMinTimestamp = record.timestamp() >= minTimestamp
        val tp = new TopicPartition(record.topic, record.partition)
        if (record.offset > progress._1) {
          progress = (record.offset, progress._2)
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

