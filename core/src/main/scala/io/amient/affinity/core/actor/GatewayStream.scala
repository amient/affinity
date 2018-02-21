package io.amient.affinity.core.actor

import java.util.concurrent.{Executors, TimeUnit}

import akka.event.Logging
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}
import io.amient.affinity.core.util.{EventTime, OutputDataStream, TimeRange}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait GatewayStream extends Gateway {

  @volatile private var closed = false
  @volatile private var clusterSuspended = true
  @volatile private var processingPaused = true
  private val lock = new Object

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  type InputStreamProcessor[K, V] = Record[K, V] => Unit

  private val declaredInputStreamProcessors = new mutable.ListBuffer[RunnableInputStream[_, _]]

  def output[K: ClassTag, V: ClassTag](streamIdentifier: String): OutputDataStream[K, V] = {
    val streamConfig = LogStorage.Conf.apply(config.getConfig(s"affinity.node.gateway.stream.$streamIdentifier"))
    val keySerde: AbstractSerde[K] = Serde.of[K](config)
    val valSerde: AbstractSerde[V] = Serde.of[V](config)
    new OutputDataStream(keySerde, valSerde, streamConfig)
  }

  /**
    * Create an input stream handler which will be managed by the gateway by giving it a processor function
    * @param streamIdentifier id of the stream configuration object
    * @param processor a function that takes (Record[K,V]) and returns Boolean signal that informs the committer
    * @tparam K
    * @tparam V
    */
  def input[K: ClassTag, V: ClassTag](streamIdentifier: String)(processor: Record[K, V] => Unit): Unit = {
    val streamConfig = LogStorage.Conf.apply(config.getConfig(s"affinity.node.gateway.stream.$streamIdentifier"))
    val keySerde: AbstractSerde[K] = Serde.of[K](config)
    val valSerde: AbstractSerde[V] = Serde.of[V](config)
    declaredInputStreamProcessors += new RunnableInputStream[K, V](streamIdentifier, keySerde, valSerde, streamConfig, processor)
  }

  val inputStreamManager = new Thread {
    override def run(): Unit = {
      val inputStreamProcessors = declaredInputStreamProcessors.result()
      val inputStreamExecutor = Executors.newFixedThreadPool(inputStreamProcessors.size)
      try {
        inputStreamProcessors.foreach(inputStreamExecutor.submit)
        while (!closed) {
          //FIXME kafka consumers may hang in the poll() method so we need something to nudge it like inputStreamProcessors.foreach(_.wakeup())
          inputStreamProcessors.foreach { p =>
            p.synchronized(p.notify())
          }
          lock.synchronized(lock.wait(1000))
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
    super.preStart()
  }

  override def postStop(): Unit = {
    closed = true
    try {
      lock.synchronized(lock.notify())
    } finally {
      super.postStop()
    }
  }

  override def onClusterStatus(suspended: Boolean) = synchronized {
    if (clusterSuspended != suspended) {
      this.clusterSuspended = suspended
      super.onClusterStatus(suspended)
      lock.synchronized(lock.notify())
    }
  }

  class RunnableInputStream[K, V](identifier: String,
                                  keySerde: AbstractSerde[K],
                                  valSerde: AbstractSerde[V],
                                  streamConfig: LogStorageConf,
                                  processor: InputStreamProcessor[K, V]) extends Runnable {

    val minTimestamp = streamConfig.MinTimestamp()
    val consumer = LogStorage.newInstance(streamConfig)

    override def run(): Unit = {
      try {
        consumer.reset(TimeRange.since(minTimestamp))
        log.info(s"Initializing input stream processor: $identifier, starting from: ${EventTime.local(minTimestamp)}, details: ${streamConfig}")
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
          for (record <- consumer.fetch(true)) {
            val key: K = keySerde.fromBytes(record.key)
            val value: V = valSerde.fromBytes(record.value)
            processor(new Record(key, value, record.timestamp))
          }
          //FIXME hardcoded commit interval
          val now = System.currentTimeMillis()
          if (now - lastCommit > 10000) {
            lastCommit = now
            //FIXME at-most-once processing guarantees: here should be a call to the processor to flush if has something to
            consumer.commit()
          }

        }
      } catch {
        case NonFatal(e) => log.error(e, s"Input stream processor: $identifier")
        case _: InterruptedException =>
      } finally {
        consumer.close()
        keySerde.close()
        valSerde.close()
        log.info(s"Finished input stream processor: $identifier (closed = $closed)")
      }
    }
  }


}

