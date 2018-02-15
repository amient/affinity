package io.amient.affinity.stream

import java.util
import java.util.Properties

import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.{EventTime, TimeRange}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class BinaryStreamImpl(conf: StorageConf) extends BinaryStream with ConsumerRebalanceListener {

  private val log = LoggerFactory.getLogger(classOf[BinaryStreamImpl])

  val kafkaStorageConf = KafkaStorage.Conf(conf)

  val topic = kafkaStorageConf.Topic()

  private val producerConfig = new Properties() {
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
  private val partitionProgress = new mutable.HashMap[Int, Long]()
  private var closed = false
  private var range: TimeRange = TimeRange.ALLTIME

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  override def subscribe(minTimestamp: Long): Unit = {
    range = new TimeRange(minTimestamp, Long.MaxValue)
    kafkaConsumer.subscribe(List(topic))
  }

  override def scan(partition: Int, range: TimeRange): Unit = {
    val tp = new TopicPartition(topic, partition)
    this.range = new TimeRange(range.start, math.min(range.end, EventTime.unix))
    kafkaConsumer.assign(List(tp))
    onPartitionsAssigned(List(tp))
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
    partitions.foreach(tp => partitionProgress.remove(tp.partition))
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
    partitions.foreach {
      tp =>
        val beginOffset: Long = kafkaConsumer.beginningOffsets(List(tp))(tp)
        val nextOffset: Long = Option(kafkaConsumer.committed(tp)).map(_.offset() + 1).getOrElse(0)
        val minOffset: Long = math.max(nextOffset, beginOffset)
        val maxOffset: Long = kafkaConsumer.endOffsets(List(tp))(tp) - 1
        val (startOffset: Long, stopOffset: Long) = if (range == TimeRange.ALLTIME) {
          (minOffset, Long.MaxValue)
        } else {
          this.range = range
          (kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.start))).get(tp) match {
            case null => minOffset
            case some => some.offset() // inclusive of the time range start
          }, kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.end))).get(tp) match {
            case null => maxOffset
            case some => some.offset() - 1 //exclusive of the time range end
          })
        }
        if (stopOffset >= startOffset) {
          kafkaConsumer.seek(tp, startOffset)
          partitionProgress.put(tp.partition, stopOffset)
          log.debug(s"Scanning partition=${tp.partition()}, range=${range.start}:${range.end} ==> offsets=$startOffset:$stopOffset")
        }
    }
  }

  override def fetch(): util.Iterator[BinaryRecord] = {
    if (partitionProgress.isEmpty) return null
    val kafkaRecords = kafkaConsumer.poll(6000)
    kafkaRecords.iterator.filter { record =>
      if (!partitionProgress.contains(record.partition)) {
        false
      } else {
        if (record.offset >= partitionProgress(record.partition)) partitionProgress.remove(record.partition)
        record.timestamp >= range.start && record.timestamp <= range.end
      }
    }.map {
      case r => new BinaryRecord(r.key, r.value, r.timestamp)
    }
  }

  def commit() = kafkaConsumer.commitAsync()

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

