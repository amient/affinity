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

package io.amient.affinity.kafka;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public interface KafkaClient extends Serializable {
    public static long LATEST_TIME = -1L;
    public static long EARLIEST_TIME = -2L;


    int refreshLeaderBackoffMs();
    int refreshLeaderMaxRetries();


    Map<Integer, Optional<KafkaBroker>> getLeaders() throws IOException;
//        case e: LeaderNotAvailableException => throw new IOException(e)
//        case e: NotLeaderForPartitionException => throw new IOException(e)
//        case e: ConnectException => throw new IOException(e)


    Map<Integer, Long> topicOffsets(Long time, Map<Integer, Optional<KafkaBroker>> leaders);

    KafkaFetcher connect(KafkaBroker broker, KafkaTopicAndPartition tap) throws IOException;
//      case e: LeaderNotAvailableException => throw new IOException(e)
//      case e: NotLeaderForPartitionException => throw new IOException(e)
//      case e: ConnectException => throw new IOException(e)


}

//object KafkaRDD {
//
//  private def metadata[E](config: KafkaConfig, e: (SimpleConsumer) => E): E = {
//    val it = Random.shuffle(config.brokerList).iterator.flatMap { broker => {
//      try {
//        val (host: String, port: Int) = broker.split(":") match {
//          case Array(host) => (host, 9092)
//          case Array(host, port) => (host, port.toInt)
//        }
//        val consumer = new SimpleConsumer(host, port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
//        try {
//          Some(e(consumer))
//        } finally {
//          consumer.close()
//        }
//      } catch {
//        case e: Throwable => {
//          e.printStackTrace()
//          None
//        }
//      }
//    }
//    }
//    if (it.hasNext) it.next else throw new BrokerNotAvailableException("operation failed for all brokers")
//  }
//
//  def getLeaders(topic: String, config: KafkaConfig): Map[Int, Option[BrokerEndPoint]] = metadata(config, (consumer) => {
//    val topicMeta = consumer.send(new TopicMetadataRequest(Seq(topic), 0)).topicsMetadata.head
//    ErrorMapping.maybeThrowException(topicMeta.errorCode)
//    topicMeta.partitionsMetadata.map { partitionMeta =>
//      ErrorMapping.maybeThrowException(partitionMeta.errorCode)
//      (partitionMeta.partitionId, partitionMeta.leader)
//    }.toMap
//  })
//
//
//  private def simpleConsumer(broker: BrokerEndPoint, config: KafkaConfig): SimpleConsumer = {
//    new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
//  }
//
//  private def partitionOffset(tap: TopicAndPartition, time: Long, consumer: SimpleConsumer): Long = {
//
//    val partitionOffsetsResponse = consumer.getOffsetsBefore(OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(time, 1))))
//      .partitionErrorAndOffsets(tap)
//    ErrorMapping.maybeThrowException(partitionOffsetsResponse.error)
//    partitionOffsetsResponse.offsets.head
//  }
//
//  def topicOffsets(topic: String, time: Long, leaders: Map[Int, Option[BrokerEndPoint]], config: KafkaConfig): Map[Int, Long] =
//    leaders.par.map {
//      case (partition, None) =>
//        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
//      case (partition, Some(leader)) =>
//        val consumer = simpleConsumer(leader, config)
//        try {
//          (partition, partitionOffset(TopicAndPartition(topic, partition), time, consumer))
//        } finally {
//          consumer.close()
//        }
//    }.seq
//
//
//  /** Write contents of this RDD to Kafka messages by creating a Producer per partition. */
//  def produceToKafka(kafkaConfig: KafkaConfig, topic: String, rdd: RDD[(Array[Byte], Array[Byte])]) {
//
//    val produced = rdd.context.accumulator(0L, "Produced Messages")
//
//    def write(context: TaskContext, iter: Iterator[(Array[Byte], Array[Byte])]) {
//      val config = new ProducerConfig(new Properties() {
//        put("metadata.broker.list", kafkaConfig.brokers)
//        put("compression.codec", "snappy")
//        put("linger.ms", "200")
//        put("batch.size", "10000")
//        put("acks", "0")
//      })
//
//      val producer = new Producer[Array[Byte], Array[Byte]](config)
//
//      try {
//        var messages = 0L
//        iter.foreach { case (key, msg) => {
//          if (context.isInterrupted) sys.error("interrupted")
//          producer.send(new KeyedMessage(topic, key, msg))
//          messages += 1
//        }
//        }
//        produced += messages
//      } finally {
//        producer.close()
//      }
//    }
//
//    println(s"Producing into topic `${topic}` at ${kafkaConfig.brokers} ...")
//    rdd.context.runJob(rdd, write _)
//    println(s"Produced ${produced.value} messages into topic `${topic}` at ${kafkaConfig.brokers}.")
//  }
//
//}
