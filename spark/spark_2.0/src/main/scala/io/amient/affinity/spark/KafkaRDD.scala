///*
// * Copyright 2016 Michal Harish, michal.harish@gmail.com
// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.amient.util.spark
//
//import java.lang.Long
//
//import io.amient.affinity.kafka._
//import io.amient.affinity.spark.KafkaSplit
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.collection.ExternalAppendOnlyMap
//import org.apache.spark.{Partition, SparkContext, TaskContext}
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
//class KafkaRDD(sc: SparkContext, client: KafkaClient, val extraParallelism: Int = 1, val selectPartition: Int = -1)
//  extends RDD[(ByteKey, PayloadAndOffset)](sc, Nil) {
//
//  def compact(rdd: KafkaRDD): RDD[(ByteKey, PayloadAndOffset)] = mapPartitions { rawMessages =>
//    val compactor = (m1: PayloadAndOffset, m2: PayloadAndOffset) => if (m1.offset > m2.offset) m1 else m2
//    val spillMap = new ExternalAppendOnlyMap[ByteKey, PayloadAndOffset, PayloadAndOffset]((v) => v, compactor, compactor)
//    spillMap.insertAll(rawMessages)
//    spillMap.iterator
//  }
//
//  protected def getPartitions: Array[Partition] = {
//
//    val startOffsets = client.topicOffsets(KafkaClient.EARLIEST_TIME).asScala
//    val stopOffsets = client.topicOffsets(KafkaClient.LATEST_TIME).asScala
//
//    val offsets: mutable.Map[Integer, (Long, Long)] = startOffsets.map {
//      case (partition, startOffset) => (partition, (startOffset, stopOffsets(partition)))
//    }
//    var index = -1
//    offsets.flatMap { case (partition, (startOffset, stopOffset)) =>
//      if (selectPartition >= 0 && selectPartition != partition) {
//        Seq()
//      } else {
//        if (extraParallelism < 2) {
//          index += 1
//          Seq(new KafkaSplit(id, index, partition, startOffset, stopOffset))
//        } else {
//          val range = (stopOffset - startOffset)
//          val interval = math.ceil(range.toDouble / extraParallelism).toLong
//          (0 to extraParallelism - 1).map { i =>
//            val startMarker = interval * i + startOffset
//            val endMarker = math.min(startMarker + interval, stopOffset)
//            index += 1
//            val split = new KafkaSplit(id, index, partition, startMarker, endMarker)
//            split
//          }
//        }
//      }
//    }.toArray
//  }
//
//  override def compute(split: Partition, context: TaskContext): Iterator[(ByteKey, PayloadAndOffset)] = {
//    val kafkaSplit = split.asInstanceOf[KafkaSplit]
//    val partition = kafkaSplit.partition
//    val startOffset = kafkaSplit.startOffset
//    val stopOffset = kafkaSplit.stopOffset
//    val iterator = client.iterator(partition, startOffset, stopOffset)
//    context.addTaskCompletionListener(_ => client.close())
//    iterator.asScala.map { keyPayloadAndOffset =>
//      (keyPayloadAndOffset.key, keyPayloadAndOffset.payloadAndOffset)
//    }
//  }
//}
//
////object KafkaRDD {
////
////  /** Write contents of this RDD to Kafka messages by creating a Producer per partition. */
////  def produceToKafka(kafkaConfig: KafkaConfig, topic: String, rdd: RDD[(Array[Byte], Array[Byte])]) {
////
////    val produced = rdd.context.accumulator(0L, "Produced Messages")
////
////    def write(context: TaskContext, iter: Iterator[(Array[Byte], Array[Byte])]) {
////      val config = new ProducerConfig(new Properties() {
////        put("metadata.broker.list", kafkaConfig.brokers)
////        put("compression.codec", "snappy")
////        put("linger.ms", "200")
////        put("batch.size", "10000")
////        put("acks", "0")
////      })
////
////      val producer = new Producer[Array[Byte], Array[Byte]](config)
////
////      try {
////        var messages = 0L
////        iter.foreach { case (key, msg) => {
////          if (context.isInterrupted) sys.error("interrupted")
////          producer.send(new KeyedMessage(topic, key, msg))
////          messages += 1
////        }
////        }
////        produced += messages
////      } finally {
////        producer.close()
////      }
////    }
////
////    println(s"Producing into topic `${topic}` at ${kafkaConfig.brokers} ...")
////    rdd.context.runJob(rdd, write _)
////    println(s"Produced ${produced.value} messages into topic `${topic}` at ${kafkaConfig.brokers}.")
////  }
////
////}
//
