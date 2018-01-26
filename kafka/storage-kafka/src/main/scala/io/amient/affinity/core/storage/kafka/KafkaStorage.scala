/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.storage.kafka

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import java.util.{Optional, Properties}

import com.typesafe.config.Config
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.{ObservableState, StateConf, Storage}
import io.amient.affinity.core.util.{EventTime, MappedJavaFuture}
import io.amient.affinity.stream.Record
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ConfigEntry, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{TopicExistsException, WakeupException}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls


object KafkaStorage {

  object StateConf extends KafkaStateConf {
    override def apply(config: Config): KafkaStateConf = new KafkaStateConf().apply(config)
  }

  class KafkaStateConf extends CfgStruct[KafkaStateConf](classOf[StateConf]) {
    val Storage = struct("storage", new KafkaStorageConf, true)
  }

  object KafkaStorageConf extends KafkaStorageConf {
    override def apply(config: Config): KafkaStorageConf = new KafkaStorageConf().apply(config)
  }

  class KafkaStorageConf extends CfgStruct[KafkaStorageConf](classOf[StorageConf]) {
    val Topic = string("kafka.topic", true)
    val ReplicationFactor = integer("kafka.topic.replication.factor", 1)
    val BootstrapServers = string("kafka.bootstrap.servers", true)
    val Producer = struct("kafka.producer", new KafkaProducerConf, false)
    val Consumer = struct("kafka.consumer", new KafkaConsumerConf, false)
  }

  class KafkaProducerConf extends CfgStruct[KafkaProducerConf](Cfg.Options.IGNORE_UNKNOWN)

  class KafkaConsumerConf extends CfgStruct[KafkaConsumerConf](Cfg.Options.IGNORE_UNKNOWN) {
    val GroupId = string("group.id", false)
  }

}

class KafkaStorage(id: String, stateConf: StateConf, partition: Int, numPartitions: Int) extends Storage(id, stateConf, partition) {

  val log = LoggerFactory.getLogger(classOf[KafkaStorage])

  private val conf = KafkaStorage.StateConf(stateConf).Storage
  final val readonly: Boolean = stateConf.External()
  final val topic = conf.Topic()
  final val ttlMs = stateConf.TtlSeconds() * 1000L
  final val minTimestamp: Long = math.max(stateConf.MinTimestampUnixMs(),
    if (ttlMs < 0) 0L else EventTime.unix() - ttlMs)


  val consumerProps = new Properties() {
    if (conf.Consumer.isDefined) {
      val consumerConfig = conf.Consumer.config()
      if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
      consumerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", conf.BootstrapServers())
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  ensureCorrectTopicConfiguration()

  protected val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] =
    if (readonly) null else {
      val producerProps = new Properties() {
        if (conf.Producer.isDefined) {
          val producerConfig = conf.Producer.config()
          if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
          if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("key.serializer cannot be overriden for KafkaStroage producer")
          if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("value.serializer cannot be overriden for KafkaStroage producer")
          producerConfig.entrySet().foreach { case (entry) =>
            put(entry.getKey, entry.getValue.unwrapped())
          }
        }
        put("bootstrap.servers", conf.BootstrapServers())
        put("key.serializer", classOf[ByteArraySerializer].getName)
        put("value.serializer", classOf[ByteArraySerializer].getName)
      }
      require(producerProps.getProperty("acks", "1") != "0", "State store kafka producer acks cannot be configured to 0, at least 1 ack is required for consistency")
      new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    }

  @volatile private var tailing = true

  @volatile private var consuming = false

  @volatile private var state: ObservableState[_] = null

  private val consumerError = new AtomicReference[Throwable](null)

  private val consumer = new Thread {

    val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)

    override def run(): Unit = {
      try {
        val tp = new TopicPartition(topic, partition)
        val consumerPartitions = util.Arrays.asList(tp)

        kafkaConsumer.assign(consumerPartitions)
        val endOffset: Long = kafkaConsumer.endOffsets(consumerPartitions).get(tp) - 1

        log.debug(s"End offset for $topic/$partition: $endOffset")

        memstore.getCheckpoint match {
          case checkpoint if checkpoint.offset <= 0 =>
            log.info(s"Rewinding $tp to event time: " + EventTime.local(minTimestamp))
            kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(minTimestamp))).get(tp) match {
              case null => kafkaConsumer.seekToEnd(consumerPartitions)
              case offsetAndTime => kafkaConsumer.seek(tp, offsetAndTime.offset)
            }
          case checkpoint =>
            log.info(s"Seeking ${checkpoint.offset} into $tp")
            kafkaConsumer.seek(tp, checkpoint.offset)
        }

        while (true) {

          if (isInterrupted) throw new InterruptedException

          consuming = true
          var lastProcessedOffset: Long = endOffset
          while (consuming) {

            if (isInterrupted) throw new InterruptedException

            try {
              val records = kafkaConsumer.poll(500)
              for (r <- records.iterator()) {
                lastProcessedOffset = r.offset()
                //for tailing state it means either it is a) replica b) external
                if (r.value == null) {
                  memstore.unload(r.key, r.offset())
                  if (tailing) state.internalPush(r.key, Optional.empty[Array[Byte]])
                } else {
                  memstore.load(r.key, r.value, r.offset(), r.timestamp())
                  if (tailing) state.internalPush(r.key, Optional.of(r.value))
                }
              }
              if (!tailing && lastProcessedOffset >= endOffset) {
                consuming = false
              }
            } catch {
              case _: WakeupException => throw new InterruptedException
              case e: Throwable =>
                synchronized {
                  consumerError.set(e)
                  notify //boot failure
                }
            }
          }

          synchronized {
            notify() //boot complete
            wait() //wait for tail instruction
          }
        }
      } catch {
        case _: InterruptedException => return
        case e: Throwable => consumerError.set(e)
      } finally {
        kafkaConsumer.close()
      }
    }
  }

  private[affinity] def init(state: ObservableState[_]): Unit = {
    this.state = state
    consumer.start()
  }

  private[affinity] def boot(): Unit = {
    consumer.synchronized {
      if (tailing) {
        tailing = false
        while (true) {
          consumer.wait(1000)
          if (consumerError.get != null) {
            consumer.kafkaConsumer.wakeup()
            throw consumerError.get
          } else if (!consuming) {
            return
          }
        }
      }
    }
  }

  private[affinity] def tail(): Unit = {
    consumer.synchronized {
      if (!tailing) {
        tailing = true
        consumer.notify
      }
    }
  }

  override def close(): Unit = {
    try {
      consumer.interrupt()
      consumer.kafkaConsumer.wakeup()
      if (!readonly) kafkaProducer.close
    } finally {
      super.close()
    }
  }

  def write(record: Record[Array[Byte], Array[Byte]]): Future[java.lang.Long] = {
    if (readonly) throw new RuntimeException("Modification attempt on a readonly storage")
    val p = if (partition < 0) defaultPartitioner.partition(record.key, numPartitions) else partition
    val producerRecord = new ProducerRecord(topic, p, record.timestamp, record.key, record.value)
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(producerRecord)) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

  def delete(key: Array[Byte]): Future[java.lang.Long] = {
    if (readonly) throw new RuntimeException("Modification attempt on a readonly storage")
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(new ProducerRecord(topic, partition, key, null))) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

  private def ensureCorrectTopicConfiguration() {
    val adminProps = new Properties() {
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, conf.BootstrapServers())
    }
    val admin = AdminClient.create(adminProps)
    try {
      val adminTimeoutMs = 15000
      val compactionPolicy = (if (ttlMs > 0) "compact,delete" else "compact")
      val replicationFactor = conf.ReplicationFactor().toShort
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
          throw new IllegalStateException(s"Kafka topic $topic has ${description.partitions().size()}, expecting: $numPartitions")
        }
        log.debug(s"Checking that topic $topic has correct replication factor: ${replicationFactor}")
        val actualReplFactor = description.partitions().get(0).replicas().size()
        if ( actualReplFactor < replicationFactor) {
          throw new IllegalStateException(s"Kafka topic $topic has $actualReplFactor, expecting: $replicationFactor")
        }
        log.debug(s"Checking that topic $topic contains all required configs: ${topicConfigs}")
        val topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
        val actualConfig = admin.describeConfigs(List(topicConfigResource))
          .values().head._2.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
        val configOutOfSync = topicConfigs.exists { case (k,v) => actualConfig.get(k).value() != v }
        if (readonly) {
          if (configOutOfSync) log.warn(s"External State configuration doesn't match the state configuration: $topicConfigs")
        } else if (configOutOfSync) {
          val entries: util.Collection[ConfigEntry] = topicConfigs.map { case (k,v) => new ConfigEntry(k,v) }
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
