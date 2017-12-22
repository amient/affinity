package io.amient.affinity.core.actor

import java.util.concurrent.Executors

import akka.event.Logging
import io.amient.affinity.core.Record
import io.amient.affinity.core.serde.Serde
import io.amient.affinity.core.storage.EventTime
import io.amient.affinity.kafka.ManagedKafkaConsumer

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait GatewayStream extends Gateway {

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  private val inputStreamProcessors = new mutable.ListBuffer[Runnable]

  private val inputStreamExecutor = Executors.newCachedThreadPool()

  @volatile private var running = true

  def stream[K: ClassTag, V: ClassTag](identifier: String)(processor: Record[K, V] => Unit): Unit = {
    val streamConfig = config.getConfig(s"affinity.node.gateway.stream.$identifier")
    inputStreamProcessors += new Runnable {
      val inputTopics = streamConfig.getStringList("topics").toSet
      val minTimestamp = if (streamConfig.hasPath("min.timestamp")) streamConfig.getLong("min.timestamp") else 0L
      val keySerde = Serde.of[K](config)
      val valSerde = Serde.of[V](config)
      override def run(): Unit = {
        val consumer: ManagedKafkaConsumer = Class.forName("io.amient.affinity.kafka.ManagedKafkaConsumerImpl")
          .asSubclass(classOf[ManagedKafkaConsumer]).newInstance()
        try {
          consumer.initialize(streamConfig.getConfig("consumer"), inputTopics)
          log.info(s"Starting input stream processor: $identifier, min.timestamp: ${EventTime.localTime(minTimestamp)}, topics: $inputTopics")
          while (running) {
            for (record: Record[Array[Byte], Array[Byte]] <- consumer.fetch(minTimestamp)) {
              val key: K = keySerde.fromBytes(record.key)
              val value: V = valSerde.fromBytes(record.value)
              processor(new Record(key, value, record.timestamp))
            }
          }
        } catch {
          case NonFatal(e) => log.error(e, s"Input stream processor: $identifier")
        } finally {
          consumer.close()
          log.info(s"Finished input stream processor: $identifier")
        }
      }
    }
  }

  override def preStart(): Unit = {
    //FIXME this code is prone to hangs - there is no way of controlling an actor's preStart activity
    //FIXME also this code assumes the key is avro serialized
//    maybeStartMigration()
    super.preStart()
  }

  override def postStop(): Unit = {
    running = false
    super.postStop()
  }

  override def onClusterStatus(suspended: Boolean) = {
    super.onClusterStatus(suspended)
    if (!suspended && running) {
      maybeStartInputStreams()
      //FIXME deal with suspended state while running, e.g. pause the underlying kafka consumers
    }
  }

  private def maybeStartInputStreams(): Unit = {
    inputStreamProcessors.foreach(inputStreamExecutor.submit)
  }

//  private def maybeStartMigration(): Unit = {
//    if (config.hasPath("affinity.keyspace")) {
//      val workers = (for (
//        keyspaceToMigrate: String <- config.getObject("affinity.keyspace").keySet().toList;
//        stateToMigrate: String <- config.getObject(s"affinity.keyspace.$keyspaceToMigrate.state").keySet().toList
//      ) yield {
//        val keyspaceConfig = Keyspace.KeyspaceConf(config.getConfig(s"affinity.keyspace.${keyspaceToMigrate}"))
//        val stateConfig = keyspaceConfig.State(stateToMigrate)
//        val kafkaOutputStateConf = KafkaStorage.StateConf(stateConfig)
//        if (!kafkaOutputStateConf.Storage.OldTopic.isDefined) {
//          None
//        } else {
//          val targetNumParts = keyspaceConfig.NumPartitions()
//          val minTimestamp = stateConfig.MinTimestamp()
//          val kafkaInputStateConf: KafkaStateConf = KafkaStorage.StateConf(kafkaOutputStateConf.config())
//          val inputTopic = kafkaOutputStateConf.Storage.OldTopic()
//          if (kafkaOutputStateConf.Storage.Topic() != inputTopic) {
//            kafkaInputStateConf.Storage.Topic.setValue(inputTopic)
//            val keySerde = AvroSerde.create(config)
//            Some(new Repartitioner(keySerde, kafkaInputStateConf, minTimestamp, kafkaOutputStateConf, targetNumParts))
//          } else {
//            None
//          }
//        }
//      }).flatten
//
//      if (workers.size > 0) {
//        log.info(s"MIGRATING WITH ACTIVE WORKERS ${workers.filter(_.active).size}")
//        while (workers.filter(_.active).size > 0) {
//          workers.filter(_.active).foreach { worker =>
//            val copied = worker.copy()
//            if (copied == 0 && worker.maxLag() <= 1) {
//              worker.close()
//            } else {
//              log.info(s"COPIED $copied RECORD FROM ${worker.inputTopic} TO ${worker.outputTopic}")
//            }
//          }
//        }
//        log.info(s"MIGRATION COMPLETED")
//      }
//    }
//
//  }

}

