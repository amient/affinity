package io.amient.affinity.stream

import java.util
import java.util.Properties

import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.TimeRange
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class BinaryStreamImpl(conf: StorageConf) extends BinaryStream {

  private val log = LoggerFactory.getLogger(classOf[BinaryStreamImpl])

  val kafkaStorageConf = KafkaStorage.Conf(conf)

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
  private var range: TimeRange = TimeRange.ALLTIME
  private var progress: (Long, Long) = (0, -1)

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  override def subscribe(minTimestamp: Long): Unit = {
    range = new TimeRange(minTimestamp, Long.MaxValue)
    kafkaConsumer.subscribe(List(topic))
  }

  override def scan(partition: Int, range: TimeRange): Unit = {
    val tp = new TopicPartition(topic, partition)
    kafkaConsumer.assign(List(tp))
    val (minOffset, maxOffset) = (kafkaConsumer.beginningOffsets(List(tp))(tp), kafkaConsumer.endOffsets(List(tp))(tp))
    val (startOffset: Long, endOffset: Long) = if (range == TimeRange.ALLTIME) {
      (minOffset, maxOffset)
    } else {
      this.range = range
      (kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.start))).get(tp) match {
        case null => minOffset
        case some => some.offset()
      }, kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.end))).get(tp) match {
        case null => maxOffset
        case some => some.offset()
      })
    }
    kafkaConsumer.seek(tp, startOffset)
    log.debug(s"Scanning partition=$partition, range=${range.start}:${range.end} ==> offsets=$startOffset:$endOffset")
    progress = (startOffset - 1, endOffset - 1)
  }

  override def lag(): Long = {
    progress._2 - progress._1
  }

  override def fetch(): util.Iterator[BinaryRecord] = {
    val kafkaRecords = kafkaConsumer.poll(6000)
    var fastForwarded = false
    val filteredKafkaRecords = kafkaRecords.iterator().filter { record =>
      val isAfterMinTimestamp = record.timestamp() >= range.start
      val isBeforeMaxTimestamp = record.timestamp() <= range.end
      if (record.offset > progress._1) progress = (record.offset, progress._2)
      if (!isAfterMinTimestamp && !fastForwarded) {
        kafkaConsumer.offsetsForTimes(Map(new TopicPartition(record.topic(), record.partition()) -> new java.lang.Long(range.start))).foreach {
          case (tp, oat) => if (oat.offset > record.offset) {
            log.info(s"Fast forward partition ${record.topic()}/${record.partition()} because record.timestamp(${record.timestamp()}) < ${range.start}")
            kafkaConsumer.seek(tp, oat.offset())
          }
        }
        fastForwarded = true
      }
      isAfterMinTimestamp && isBeforeMaxTimestamp
    }

    filteredKafkaRecords.map {
      case r => new BinaryRecord(r.key(), r.value(), r.timestamp())
    }
  }

  def commit() = {
    kafkaConsumer.commitAsync()
  }

  def active() = !closed

  private var producerActive = false

  lazy private val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)

  override def publish(iter: util.Iterator[BinaryRecord]): Long = {
    producerActive = true
    val partitioner = getDefaultPartitioner()
    val numPartitions = getNumPartitions()

    var messages = 0L
    while (iter.hasNext) {
      val record = iter.next()
      val producerRecord: ProducerRecord[Array[Byte], Array[Byte]] = if (record.key == null) {
        new ProducerRecord(topic, null, record.timestamp, null, record.value)
      } else {
        val partition = partitioner.partition(record.key, numPartitions)
        new ProducerRecord(topic, partition, record.timestamp, record.key, record.value)
      }
      producer.send(producerRecord)
      messages += 1
    }
    messages

  }

  override def flush() = if (producerActive) {
    producer.flush()
  }

  override def close(): Unit = {
    try kafkaConsumer.close() finally try if (producerActive) producer.close() finally {
      closed = true
    }
  }

}

