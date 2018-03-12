/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.actor

import java.io.Closeable
import java.util.concurrent.{Executors, TimeUnit}

import akka.event.Logging
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}
import io.amient.affinity.core.util.{CompletedJavaFuture, EventTime, OutputDataStream, TimeRange}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

trait GatewayStream extends Gateway {

  @volatile private var closed = false
  @volatile private var clusterSuspended = true

  private val lock = new Object

  private val log = Logging.getLogger(context.system, this)

  private val config = context.system.settings.config

  type InputStreamProcessor[K, V] = Record[K, V] => Future[Any]

  private val declaredInputStreamProcessors = new mutable.ListBuffer[RunnableInputStream[_, _]]

  private val declardOutputStreams = new ListBuffer[OutputDataStream[_, _]]

  lazy val outpuStreams: ParSeq[OutputDataStream[_, _]] = declardOutputStreams.result().par

  def output[K: ClassTag, V: ClassTag](streamIdentifier: String): OutputDataStream[K, V] = {
    val streamConfig = LogStorage.StorageConf(config.getConfig(s"affinity.node.gateway.stream.$streamIdentifier"))
    val keySerde: AbstractSerde[K] = Serde.of[K](config)
    val valSerde: AbstractSerde[V] = Serde.of[V](config)
    val outpuDataStream = new OutputDataStream(keySerde, valSerde, streamConfig)
    declardOutputStreams += outpuDataStream
    outpuDataStream
  }

  /**
    * Create an input stream handler which will be managed by the gateway by giving it a processor function
    *
    * @param streamIdentifier id of the stream configuration object
    * @param processor        a function that takes (Record[K,V]) and returns Boolean signal that informs the committer
    * @tparam K
    * @tparam V
    */
  def input[K: ClassTag, V: ClassTag](streamIdentifier: String)(processor: InputStreamProcessor[K, V]): Unit = {
    val streamConfig = LogStorage.StorageConf(config.getConfig(s"affinity.node.gateway.stream.$streamIdentifier"))
    val keySerde: AbstractSerde[K] = Serde.of[K](config)
    val valSerde: AbstractSerde[V] = Serde.of[V](config)
    declaredInputStreamProcessors += new RunnableInputStream[K, V](streamIdentifier, keySerde, valSerde, streamConfig, processor)
  }

  val inputStreamManager = new Thread {
    override def run(): Unit = {
      val inputStreamProcessors = declaredInputStreamProcessors.result()
      if (inputStreamProcessors.isEmpty) return
      val inputStreamExecutor = Executors.newFixedThreadPool(inputStreamProcessors.size)
      try {
        inputStreamProcessors.foreach(inputStreamExecutor.submit)
        while (!closed) {
          lock.synchronized(lock.wait(1000))
        }
        inputStreamExecutor.shutdown()
        inputStreamProcessors.foreach(_.close)
        inputStreamExecutor.awaitTermination(10, TimeUnit.SECONDS) //TODO use shutdown timeout
      } finally {
        inputStreamExecutor.shutdownNow()
      }
    }
  }

  abstract override def preStart(): Unit = {
    inputStreamManager.start()
    super.preStart()
  }

  abstract override def shutdown(): Unit = {
    try {
      lock.synchronized {
        closed = true
        lock.notifyAll()
      }
      inputStreamManager.synchronized {
        inputStreamManager.join()
      }
    } finally {
      super.shutdown()
    }
  }

  abstract override def onClusterStatus(suspended: Boolean) = synchronized {
    if (clusterSuspended != suspended) {
      lock.synchronized {
        this.clusterSuspended = suspended
        lock.notifyAll()
      }
      super.onClusterStatus(suspended)
    }
  }

  class RunnableInputStream[K, V](identifier: String,
                                  keySerde: AbstractSerde[K],
                                  valSerde: AbstractSerde[V],
                                  streamConfig: LogStorageConf,
                                  processor: InputStreamProcessor[K, V]) extends Runnable with Closeable {

    val minTimestamp = streamConfig.MinTimestamp()
    val consumer = LogStorage.newInstance(streamConfig)
    //this type of buffering has quite a high memory footprint but doesn't require a data structure with concurrent access
    val work = new ListBuffer[Future[Any]]
    //TODO make hardcoded commit interval configurable
    val commitInterval = 10 seconds

    override def close(): Unit = consumer.cancel()

    override def run(): Unit = {
      implicit val executor = scala.concurrent.ExecutionContext.Implicits.global
      var lastCommit: java.util.concurrent.Future[java.lang.Long] = new CompletedJavaFuture(0L)

      try {
        consumer.reset(TimeRange.since(minTimestamp))
        log.info(s"Initializing input stream processor: $identifier, starting from: ${EventTime.local(minTimestamp)}, details: ${streamConfig}")
        var lastCommitTimestamp = System.currentTimeMillis()
        var finalized = false
        while ((!closed && !finalized) || !lastCommit.isDone) {
          //clusterSuspended is volatile so we check it for each message set, in theory this should not matter because whatever the processor() does
          //should be suspended anyway and hang so no need to do it for every record
          if (clusterSuspended) {
            log.info(s"Pausing input stream processor: $identifier")
            lock.synchronized(lock.wait())
            if (closed) return
            log.info(s"Resuming input stream processor: $identifier")
          }
          val entries = consumer.fetch(true)
          if (entries != null) for (entry <- entries) {
            //TODO we need entry.partition and use it with entry.position to provide watermark and gather it for distribution during the commit
            val key: K = keySerde.fromBytes(entry.key)
            val value: V = valSerde.fromBytes(entry.value)
            val unitOfWork = processor(new Record(key, value, entry.timestamp))
            if (!unitOfWork.isCompleted) work += unitOfWork
          }
          /*  At-least-once guarantee processing input messages
           *  Every <commitInterval> all outputs and work is flushed and then consumer is commited()
           */
          val now = System.currentTimeMillis()
          if ((closed && !finalized) || now - lastCommitTimestamp > commitInterval.toMillis) {
            //flush all outputs in parallel - these are all outputs declared in this gateway
            outpuStreams.foreach(_.flush())
            //flush all pending work accumulated in this processor only
            Await.result(Future.sequence(work.result), commitInterval)
            //commit the records processed by this processor only since the last commit
            lastCommit = consumer.commit() //trigger new commit
            //TODO here the underlying commit future would distribute on completion the partial watermark to registered accumulator actors
            //clear the work accumulator for the next commit
            work.clear
            lastCommitTimestamp = now
            if (closed) finalized = true
          }
        }

      } catch {
        case _: InterruptedException =>
        case e: Throwable => log.error(e, s"Input stream processor: $identifier")
      } finally {
        log.info(s"Finished input stream processor: $identifier (closed = $closed)")
        consumer.close()
        keySerde.close()
        valSerde.close()
      }
    }


  }


}

