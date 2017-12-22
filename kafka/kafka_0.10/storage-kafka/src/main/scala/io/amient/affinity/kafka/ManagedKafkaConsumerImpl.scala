package io.amient.affinity.kafka

import java.util
import java.util.Properties

import com.typesafe.config.Config
import io.amient.affinity.core.Record
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class ManagedKafkaConsumerImpl extends ManagedKafkaConsumer {

  private val log = LoggerFactory.getLogger(classOf[ManagedKafkaConsumerImpl])

  private var closed = true

  private var kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  override def initialize(config: Config, topics: util.Set[String]): Unit = {
    val consumerProps = new Properties() {
      require(config != null)
      config.entrySet().foreach { case entry =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
      put("key.deserializer", classOf[ByteArrayDeserializer].getName)
      put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    }
    kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    kafkaConsumer.subscribe(topics)
  }

  private var partitionProgress = new mutable.HashMap[TopicPartition, Long]()

  closed = false

  override def lag(): util.Map[String, java.lang.Long] = {
    kafkaConsumer.endOffsets(partitionProgress.keys).toList.map {
      case (tp, endOffset) => (tp, endOffset - partitionProgress(tp))
    }.groupBy(_._1.topic).mapValues(_.map(_._2).max).mapValues(new java.lang.Long(_))
  }

  override def fetch(minTimestamp: Long): util.Iterator[Record[Array[Byte], Array[Byte]]] = {
    val records: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = kafkaConsumer.poll(15000).iterator()
    val it = if (minTimestamp < 0) records else {
      var fastForwarded = false
      records.filter {
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
    }
    it.map {
      case r: ConsumerRecord[Array[Byte], Array[Byte]] =>
        new Record(r.key(), r.value(), r.timestamp())
    }
  }

  def active() = !closed

  override def close(): Unit = {
    try kafkaConsumer.close() finally closed = true
  }

}

