/**
  * KafkaRDD
  * Copyright (C) 2015 Michal Harish
  * <p/>
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  * <p/>
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  * <p/>
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package io.amient.util.spark

import java.net.ConnectException
import java.util
import java.util.{NoSuchElementException, Properties}

import io.amient.util.ByteUtils
import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.common.{BrokerNotAvailableException, ErrorMapping, LeaderNotAvailableException, NotLeaderForPartitionException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndOffset, MessageSet}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.VerifiableProperties
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag
import scala.util.Random

/*
 * This is a modified version of com.tresata.spark.kafka package which is focused on providing a view on the
 * compacted kafka topics.
 */

class KafkaConfig(@transient private val properties: Properties) extends Serializable {

  @transient private val props = new VerifiableProperties(properties)

  val clientId = props.getString("client.id", "")
  val brokers = props.getString("metadata.broker.list", "localhost:9092")
  val brokerList = brokers.split(",").toList
  val socketTimeoutMs = props.getInt("socket.timeout.ms", 30 * 1000)
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 64 * 1024)
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", 1 * 1024 * 1024)
  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", 1000)
  val refreshLeaderMaxRetries = props.getInt("refresh.leader.max.retries", 10)

  props.verify()
}

/**
  * KafkaSplit is always related to a single kafka topic. Kafka topic may have multiple partitions and these
  * can be further sub-divided multiple times to get extra parallelism. Hence the number of splits if defined by
  * number of kafka partition times the extraParallelism factor.
  *
  * @param rddId
  * @param index ∈ { i | 0 ≤ i < numPartitions * extraParallelism }
  * @param partition - kafka partition number
  * @param startOffset - offset range start - inclusive
  * @param stopOffset - offset range end - exclusive
  * @param leader - host:port of the leader broker for this partition
  */
class KafkaSplit(rddId: Int, override val index: Int,
                 val partition: Int, val startOffset: Long, val stopOffset: Long,
                 val leader: Option[BrokerEndPoint]) extends Partition {
  override def hashCode: Int = 41 * 41 * (41 + rddId) + (41 * index)

  override def toString = s"KafkaSplit(index=$index, partition=$partition [$startOffset:$stopOffset], rddId=$rddId)"
}

class ByteKey(val bytes: Array[Byte]) extends scala.Serializable {
  type A = Array[Byte]
  override def hashCode() = util.Arrays.hashCode(bytes)
  override def equals(o: Any): Boolean = o.isInstanceOf[ByteKey] && util.Arrays.equals(bytes, o.asInstanceOf[ByteKey].bytes)
}

class PayloadAndOffset(val offset: Long, val bytes: Array[Byte]) extends scala.Serializable {

}

abstract class KafkaRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                  val topic: String,
                                                  config: KafkaConfig,
                                                  val extraParallelism: Int = 1,
                                                  val sinceTimeMs: Long = OffsetRequest.EarliestTime,
                                                  val selectPartition: Int = -1
                                                 )
  extends RDD[(K, V)](sc, Nil) {

  def createDeserializer(): (ByteKey, PayloadAndOffset) => (K, V)

  import KafkaRDD._

  private def retry[E](e: => E): E = retryIfNoLeader(config, e)

  protected def getPartitions: Array[Partition] = retry {
    //TODO use all ISRs not just leader
    val leaders: Map[Int, Option[BrokerEndPoint]] = getLeaders(topic, config)
    val (startOffsets, stopOffsets) = (topicOffsets(topic, sinceTimeMs, leaders, config), topicOffsets(topic, OffsetRequest.LatestTime, leaders, config))
    val offsets = startOffsets.map {
      case (partition, startOffset) => (partition, (startOffset, stopOffsets(partition)))
    }
    var index = -1
    leaders.zipWithIndex.flatMap { case ((partition, leader), partIndex) =>
      if (selectPartition >=0 && selectPartition != partition) {
        Seq()
      } else {
        val (startOffset, stopOffset) = offsets(partition)
        if (extraParallelism < 2) {
          index += 1
          Seq(new KafkaSplit(id, index, partition, startOffset, stopOffset, leader))
        } else {
          val range = (stopOffset - startOffset)
          val interval = math.ceil(range.toDouble / extraParallelism).toLong
          (0 to extraParallelism - 1).map { i =>
            val startMarker = interval * i + startOffset
            val endMarker = math.min(startMarker + interval, stopOffset)
            index += 1
            val split = new KafkaSplit(id, index, partition, startMarker, endMarker, leader)
            split
          }
        }
      }
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val deserializer = createDeserializer()
    computeKafka(split, context).map { case(key, payload) => deserializer(key, payload) }
  }

  protected def computeKafka(split: Partition, context: TaskContext): Iterator[(ByteKey, PayloadAndOffset)] = {
    val kafkaSplit = split.asInstanceOf[KafkaSplit]
    val partition = kafkaSplit.partition
    val tap = TopicAndPartition(topic, partition)
    val startOffset = kafkaSplit.startOffset
    val stopOffset = kafkaSplit.stopOffset

    def sleep() = Thread.sleep(config.refreshLeaderBackoffMs)

    try {
      // every task reads from a single broker
      // on the first attempt we use the lead broker determined in the driver, on next attempts we ask for the lead broker ourselves
      val broker = (if (context.attemptNumber == 0) kafkaSplit.leader else None)
        .orElse(getLeaders(topic, config)(partition))
        .getOrElse(throw new LeaderNotAvailableException(s"no leader for partition ${partition}"))

      val consumer = simpleConsumer(broker, config)
      context.addTaskCompletionListener(_ => consumer.close())

      def fetch(offset: Long): MessageSet = {
        val req = new FetchRequestBuilder().clientId(config.clientId).addFetch(topic, partition, offset, config.fetchMessageMaxBytes).build
        val data = consumer.fetch(req).data(tap)
        ErrorMapping.maybeThrowException(data.error)
        data.messages
      }


      new Iterator[(ByteKey, PayloadAndOffset)] {
        private var messageAndOffset: MessageAndOffset = null
        private var offset = startOffset
        private var setIter = fetch(offset).iterator

        seek

        override def hasNext: Boolean = messageAndOffset != null

        override def next(): (ByteKey, PayloadAndOffset) = {
          if (messageAndOffset == null) {
            throw new NoSuchElementException
          } else {
            val n = (new ByteKey(ByteUtils.bufToArray(messageAndOffset.message.key)),
              new PayloadAndOffset(messageAndOffset.offset, ByteUtils.bufToArray(messageAndOffset.message.payload)))
            seek
            n
          }
        }

        private def seek {
          while (offset < stopOffset) {
            if (!setIter.hasNext) setIter = fetch(offset).iterator
            messageAndOffset = setIter.next()
            offset = messageAndOffset.nextOffset
            if (messageAndOffset.message.payload != null && messageAndOffset.offset >= startOffset) {
              return
            }
          }
          messageAndOffset = null
        }

      }
    } catch {
      case e: LeaderNotAvailableException => sleep(); throw e
      case e: NotLeaderForPartitionException => sleep(); throw e
      case e: ConnectException => sleep(); throw e
    }
  }
}

object KafkaRDD {

  private def metadata[E](config: KafkaConfig, e: (SimpleConsumer) => E): E = {
    val it = Random.shuffle(config.brokerList).iterator.flatMap { broker => {
      try {
        val (host: String, port: Int) = broker.split(":") match {
          case Array(host) => (host, 9092)
          case Array(host, port) => (host, port.toInt)
        }
        val consumer = new SimpleConsumer(host, port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
        try {
          Some(e(consumer))
        } finally {
          consumer.close()
        }
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          None
        }
      }
    }
    }
    if (it.hasNext) it.next else throw new BrokerNotAvailableException("operation failed for all brokers")
  }

  def retryIfNoLeader[E](config: KafkaConfig, e: => E): E = {
    def sleep() = Thread.sleep(config.refreshLeaderBackoffMs)
    def attempt(e: => E, nr: Int = 1): E = if (nr < config.refreshLeaderMaxRetries) {
      try (e) catch {
        case ex: LeaderNotAvailableException => sleep(); attempt(e, nr + 1)
        case ex: NotLeaderForPartitionException => sleep(); attempt(e, nr + 1)
        case ex: ConnectException => sleep(); attempt(e, nr + 1)
      }
    } else e

    attempt(e)
  }

  def getLeaders(topic: String, config: KafkaConfig): Map[Int, Option[BrokerEndPoint]] = metadata(config, (consumer) => {
    val topicMeta = consumer.send(new TopicMetadataRequest(Seq(topic), 0)).topicsMetadata.head
    ErrorMapping.maybeThrowException(topicMeta.errorCode)
    topicMeta.partitionsMetadata.map { partitionMeta =>
      ErrorMapping.maybeThrowException(partitionMeta.errorCode)
      (partitionMeta.partitionId, partitionMeta.leader)
    }.toMap
  })


  private def simpleConsumer(broker: BrokerEndPoint, config: KafkaConfig): SimpleConsumer = {
    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
  }

  private def partitionOffset(tap: TopicAndPartition, time: Long, consumer: SimpleConsumer): Long = {

    val partitionOffsetsResponse = consumer.getOffsetsBefore(OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(time, 1))))
      .partitionErrorAndOffsets(tap)
    ErrorMapping.maybeThrowException(partitionOffsetsResponse.error)
    partitionOffsetsResponse.offsets.head
  }

  def topicOffsets(topic: String, time: Long, leaders: Map[Int, Option[BrokerEndPoint]], config: KafkaConfig): Map[Int, Long] =
    leaders.par.map {
      case (partition, None) =>
        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
      case (partition, Some(leader)) =>
        val consumer = simpleConsumer(leader, config)
        try {
          (partition, partitionOffset(TopicAndPartition(topic, partition), time, consumer))
        } finally {
          consumer.close()
        }
    }.seq


  /** Write contents of this RDD to Kafka messages by creating a Producer per partition. */
  def produceToKafka(kafkaConfig: KafkaConfig, topic: String, rdd: RDD[(Array[Byte], Array[Byte])]) {

    val produced = rdd.context.accumulator(0L, "Produced Messages")

    def write(context: TaskContext, iter: Iterator[(Array[Byte], Array[Byte])]) {
      val config = new ProducerConfig(new Properties() {
        put("metadata.broker.list", kafkaConfig.brokers)
        put("compression.codec", "snappy")
        put("linger.ms", "200")
        put("batch.size", "10000")
        put("acks", "0")
      })

      val producer = new Producer[Array[Byte], Array[Byte]](config)

      try {
        var messages = 0L
        iter.foreach { case (key, msg) => {
          if (context.isInterrupted) sys.error("interrupted")
          producer.send(new KeyedMessage(topic, key, msg))
          messages += 1
        }
        }
        produced += messages
      } finally {
        producer.close()
      }
    }
    println(s"Producing into topic `${topic}` at ${kafkaConfig.brokers} ...")
    rdd.context.runJob(rdd, write _)
    println(s"Produced ${produced.value} messages into topic `${topic}` at ${kafkaConfig.brokers}.")
  }

}