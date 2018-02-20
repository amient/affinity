package io.amient.affinity.kafka

import java.{lang, util}
import java.util.Properties
import java.util.concurrent.{ExecutionException, Future, TimeUnit}

import com.typesafe.config.Config
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.storage._
import io.amient.affinity.core.util.{EventTime, MappedJavaFuture, TimeRange}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ConfigEntry, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{TopicExistsException, WakeupException}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.reflectiveCalls

object KafkaStorage {

  object StateConf extends KafkaStateConf {
    override def apply(config: Config): KafkaStateConf = new KafkaStateConf().apply(config)
  }

  class KafkaStateConf extends CfgStruct[KafkaStateConf](classOf[StateConf]) {
    val Storage = struct("storage", new Conf, true)
  }

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](classOf[LogStorageConf]) {
    val Topic = string("kafka.topic", true)
    val ReplicationFactor = integer("kafka.replication.factor", 1)
    val BootstrapServers = string("kafka.bootstrap.servers", true)
    val Producer = struct("kafka.producer", new KafkaProducerConf, false)
    val Consumer = struct("kafka.consumer", new KafkaConsumerConf, false)
  }

  class KafkaProducerConf extends CfgStruct[KafkaProducerConf](Cfg.Options.IGNORE_UNKNOWN)

  class KafkaConsumerConf extends CfgStruct[KafkaConsumerConf](Cfg.Options.IGNORE_UNKNOWN) {
    val GroupId = string("group.id", false)
  }

}

class KafkaLogStorage(conf: LogStorageConf) extends LogStorage[java.lang.Long] with ConsumerRebalanceListener {

  private val log = LoggerFactory.getLogger(classOf[KafkaLogStorage])

  val kafkaStorageConf = KafkaStorage.Conf(conf)

  val topic = kafkaStorageConf.Topic()
  val keySubject: String = s"${topic}-key"
  val valueSubject: String = s"${topic}-value"

  private val producerConfig = new Properties() {
    if (kafkaStorageConf.Producer.isDefined) {
      val producerConfig = kafkaStorageConf.Producer.config()
      if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom key.serializer")
      if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom value.serializer")
      producerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
  }

  private val consumerProps = new Properties() {
    put("auto.offset.reset", "earliest")
    if (kafkaStorageConf.Consumer.isDefined) {
      val consumerConfig = kafkaStorageConf.Consumer.config()
      if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
      consumerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  private val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  private val partitionProgress = new mutable.HashMap[Int, java.lang.Long]()
  private var closed = false
  private var range: TimeRange = TimeRange.UNBOUNDED

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  override def reset(range: TimeRange): Unit = {
    this.range = range
    kafkaConsumer.subscribe(List(topic), this)
  }

  override def reset(partition: Int, range: TimeRange): Unit = {
    val tp = new TopicPartition(topic, partition)
    this.range = range
    kafkaConsumer.assign(List(tp))
    onPartitionsAssigned(List(tp))
  }

  override def reset(partition: Int, startPosition: java.lang.Long): Unit = {
    val tp = new TopicPartition(topic, partition)
    val startOffset: Long = if (startPosition == null) kafkaConsumer.beginningOffsets(List(tp))(tp) else startPosition
    kafkaConsumer.seek(tp, startOffset)
    val maxOffset: Long = kafkaConsumer.endOffsets(List(tp))(tp)
    // exclusive of the time range end
    val stopOffset: Long = Option(kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.end))).get(tp)).map(_.offset).getOrElse(maxOffset) - 1
    log.debug(s"Reset partition=${tp.partition()} limit $startOffset:$stopOffset")
    if (stopOffset >= startOffset) {
      partitionProgress.put(tp.partition, stopOffset)
    } else {
      partitionProgress.remove(tp.partition)
    }
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
        val startOffset: Long = Option(kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.start))).get(tp)).map(_.offset).getOrElse(minOffset)
        log.debug(s"Reset partition=${tp.partition()} time range ${range.getLocalStart}:${range.getLocalEnd}")
        reset(tp.partition, startOffset)
    }
  }

  override def fetch(unbounded: Boolean): util.Iterator[LogEntry[java.lang.Long]] = {

    if (!unbounded && partitionProgress.isEmpty) return null

    val kafkaRecords = try {
      kafkaConsumer.poll(6000)
    } catch {
      case _: WakeupException => return null
    }

    kafkaRecords.iterator.filter { record =>
      if (unbounded) {
        record.timestamp >= range.start && record.timestamp <= range.end
      } else if (!partitionProgress.contains(record.partition)) {
        false
      } else {
        if (record.offset >= partitionProgress(record.partition)) partitionProgress.remove(record.partition)
        record.timestamp >= range.start && record.timestamp <= range.end
      }
    }.map {
      case r => new LogEntry(new java.lang.Long(r.offset), r.key, r.value, r.timestamp)
    }
  }

  def cancel() = kafkaConsumer.wakeup()

  def commit() = kafkaConsumer.commitAsync()

  private var producerActive = false

  lazy protected val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)

  override def append(record: Record[Array[Byte], Array[Byte]]): java.util.concurrent.Future[java.lang.Long] = {
    producerActive = true
    val producerRecord: ProducerRecord[Array[Byte], Array[Byte]] = if (record.key == null) {
      new ProducerRecord(topic, null, record.timestamp, null, record.value)
    } else {
      new ProducerRecord(topic, null, record.timestamp, record.key, record.value)
    }
    new MappedJavaFuture[RecordMetadata, java.lang.Long](producer.send(producerRecord)) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

  override def delete(key: Array[Byte]): Future[java.lang.Long] = {
    //kafka uses null value as a delete tombstone
    append(new Record[Array[Byte], Array[Byte]](key, null, EventTime.unix));
  }

  override def flush() = if (producerActive) {
    producer.flush()
  }

  override def close(): Unit = {
    try kafkaConsumer.close() finally try if (producerActive) producer.close() finally {
      closed = true
    }
  }

  override def isTombstone(entry: LogEntry[lang.Long]) = entry.value == null

  override def ensureCorrectConfiguration(ttlMs: Long, numPartitions: Int, readonly: Boolean) {
    val adminProps = new Properties() {
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorageConf.BootstrapServers())
    }
    val admin = AdminClient.create(adminProps)
    try {
      val adminTimeoutMs = 15000
      val compactionPolicy = (if (ttlMs > 0) "compact,delete" else "compact")
      val replicationFactor = kafkaStorageConf.ReplicationFactor().toShort
      val topicConfigs = Map(
        TopicConfig.CLEANUP_POLICY_CONFIG -> compactionPolicy,
        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG -> "CreateTime",
        TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG -> (if (ttlMs > 0) ttlMs else Long.MaxValue).toString,
        TopicConfig.RETENTION_MS_CONFIG -> (if (ttlMs > 0) ttlMs else Long.MaxValue).toString,
        TopicConfig.RETENTION_BYTES_CONFIG -> "-1"
      )

      var exists: Option[Boolean] = None
      while (!exists.isDefined) {
        if (admin.listTopics().names().get(adminTimeoutMs, TimeUnit.SECONDS).contains(topic)) {
          exists = Some(true)
        } else {
          val schemaTopicRequest = new NewTopic(topic, numPartitions, replicationFactor)
          schemaTopicRequest.configs(topicConfigs)
          try {
            admin.createTopics(List(schemaTopicRequest)).all.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
            log.info(s"Created topic $topic, num.partitions: $numPartitions, replication factor: $replicationFactor, configs: $topicConfigs")
            exists = Some(false)
          } catch {
            case e: ExecutionException if e.getCause.isInstanceOf[TopicExistsException] => //continue
          }
        }
      }

      if (exists.get) {
        log.debug(s"Checking that topic $topic has correct number of partitions: ${numPartitions}")
        val description = admin.describeTopics(List(topic)).values().head._2.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
        if (description.partitions().size() != numPartitions) {
          throw new IllegalStateException(s"Kafka topic $topic has ${description.partitions().size()} partitions, expecting: $numPartitions")
        }
        log.debug(s"Checking that topic $topic has correct replication factor: ${replicationFactor}")
        val actualReplFactor = description.partitions().get(0).replicas().size()
        if (actualReplFactor < replicationFactor) {
          throw new IllegalStateException(s"Kafka topic $topic has $actualReplFactor, expecting: $replicationFactor")
        }
        log.debug(s"Checking that topic $topic contains all required configs: ${topicConfigs}")
        val topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
        val actualConfig = admin.describeConfigs(List(topicConfigResource))
          .values().head._2.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
        val configOutOfSync = topicConfigs.exists { case (k, v) => actualConfig.get(k).value() != v }
        if (readonly) {
          if (configOutOfSync) log.warn(s"External State configuration doesn't match the state configuration: $topicConfigs")
        } else if (configOutOfSync) {
          val entries: util.Collection[ConfigEntry] = topicConfigs.map { case (k, v) => new ConfigEntry(k, v) }
          admin.alterConfigs(Map(topicConfigResource -> new org.apache.kafka.clients.admin.Config(entries)))
            .all().get(adminTimeoutMs, TimeUnit.MILLISECONDS)
          log.info(s"Topic $topic configuration altered successfully")
        } else {
          log.debug(s"Topic $topic configuration is up to date")
        }
      }
    } finally {
      admin.close()
    }
  }

}

