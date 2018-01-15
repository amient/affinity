package io.amient.affinity.core.actor

import java.util.concurrent.{Executors, TimeUnit}

import akka.event.Logging
import com.typesafe.config.Config
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.storage.EventTime
import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.stream.{BinaryStream, Record}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait GatewayStream extends Gateway {

  class RunnableInputStream[K, V](identifier: String,
                                  keySerde: AbstractSerde[K],
                                  valSerde: AbstractSerde[V],
                                  streamConfig: StorageConf,
                                  processor: InputStreamProcessor[K, V]) extends Runnable {
    val minTimestamp = streamConfig.MinTimestamp()
    val consumer: BinaryStream = BinaryStream.bindNewInstance(streamConfig)

    override def run(): Unit = {
      try {
        consumer.subscribe()
        log.info(s"Initializing input stream processor: $identifier, min.timestamp: ${EventTime.localTime(minTimestamp)}, details: ${streamConfig}")
        var lastCommit = System.currentTimeMillis()
        while (!closed) {
          //processingPaused is volatile so we check it for each message set, in theory this should not matter because whatever the processor() does
          //should be suspended anyway and hang so no need to do it for every record
          if (processingPaused) {
            log.info(s"Pausing input stream processor: $identifier")
            synchronized(wait())
            log.info(s"Resuming input stream processor: $identifier")
            processingPaused = false
          }
          for (record: Record[Array[Byte], Array[Byte]] <- consumer.fetch(minTimestamp)) {
            val key: K = keySerde.fromBytes(record.key)
            val value: V = valSerde.fromBytes(record.value)
            processor(new Record(key, value, record.timestamp))
          }
          val now = System.currentTimeMillis()
          // TODO configurable commit interval, currently 10s hard-coded
          if (now - lastCommit > 10000) {
            lastCommit = now
            consumer.commit()
          }
        }
      } catch {
        case NonFatal(e) => log.error(e, s"Input stream processor: $identifier")
        case _: InterruptedException =>
      } finally {
        consumer.close()
        log.info(s"Finished input stream processor: $identifier (closed = $closed)")
      }
    }
  }

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  type InputStreamProcessor[K, V] = Record[K, V] => Unit

  private val declaredInputStreamProcessors = new mutable.ListBuffer[RunnableInputStream[_, _]]

  @volatile private var closed = false
  @volatile private var clusterSuspended = true
  @volatile private var processingPaused = true

  def stream[K: ClassTag, V: ClassTag](identifier: String)(processor: Record[K, V] => Unit): Unit = {
    val streamConfig = new StorageConf().apply(config.getConfig(s"affinity.node.gateway.stream.$identifier"))
    val keySerde: AbstractSerde[K] = Serde.of[K](config)
    val valSerde: AbstractSerde[V] = Serde.of[V](config)
    declaredInputStreamProcessors += new RunnableInputStream[K, V](identifier, keySerde, valSerde, streamConfig, processor)
  }

  val inputStreamManager = new Thread {
    var suspended = true

    override def run(): Unit = {
      val inputStreamProcessors = declaredInputStreamProcessors.result()
      val inputStreamExecutor = Executors.newFixedThreadPool(inputStreamProcessors.size)
      try {
        inputStreamProcessors.foreach(inputStreamExecutor.submit)
        while (!closed) {
          //FIXME kafka consumers may hang in the poll() method so we need something to nudge it like inputStreamProcessors.foreach(_.wakeup())
          if (suspended != clusterSuspended) {
            suspended = clusterSuspended
            if (!suspended) {
              inputStreamProcessors.foreach { p =>
                p.synchronized(p.notify())
              }
            }
          }
          synchronized(wait(1000))
        }
        inputStreamExecutor.shutdown()
        inputStreamExecutor.awaitTermination(10, TimeUnit.SECONDS) //TODO use shutdown timeout
      } finally {
        inputStreamExecutor.shutdownNow()
      }
    }
  }

  override def preStart(): Unit = {
    inputStreamManager.start()
    //FIXME #89 maybeStartMigration() code can hang and there is no way of controlling an actor's preStart activity
    //    maybeStartMigration()
    super.preStart()
  }

  override def postStop(): Unit = {
    closed = true
    try {
      inputStreamManager.synchronized(inputStreamManager.notify())
    } finally {
      super.postStop()
    }
  }

  override def onClusterStatus(suspended: Boolean) = {
    super.onClusterStatus(suspended)
    this.clusterSuspended = suspended
    inputStreamManager.synchronized(inputStreamManager.notify())
  }


  //TODO #89 Internal Repartitioner Tool - also this code assumes the key is avro serialized which may not be reasonable
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
  //            if (copied == 0 && worker.maxLag() <= 0) {
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

