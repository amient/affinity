/**
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
import java.util.Optional

import io.amient.affinity.kafka._
import io.amient.affinity.spark.KafkaSplit
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._

class KafkaRDD(sc: SparkContext, client: KafkaClient, val topic: String,
               val extraParallelism: Int = 1,
               val sinceTimeMs: Long = KafkaClient.EARLIEST_TIME,
               val selectPartition: Int = -1
              )
  extends RDD[(ByteKey, PayloadAndOffset)](sc, Nil) {

  implicit def OptionalKafkaBroker(opt: Optional[KafkaBroker]): Option[KafkaBroker] = {
    if (opt.isPresent) Some(opt.get()) else None
  }


  private def retry[E](e: => E): E = {
    def sleep() = Thread.sleep(client.refreshLeaderBackoffMs())

    def attempt(e: => E, nr: Int = 1): E = if (nr < client.refreshLeaderMaxRetries()) {
      try (e) catch {
//        case ex: LeaderNotAvailableException => sleep(); attempt(e, nr + 1)
//        case ex: NotLeaderForPartitionException => sleep(); attempt(e, nr + 1)
//        case ex: ConnectException => sleep(); attempt(e, nr + 1)
        case ex: Exception => sleep(); attempt(e, nr + 1)
      }
    } else e

    attempt(e)
  }

  def compact(rdd: KafkaRDD): RDD[(ByteKey, PayloadAndOffset)] = mapPartitions { rawMessages =>
    val compactor = (m1: PayloadAndOffset, m2: PayloadAndOffset) => if (m1.offset > m2.offset) m1 else m2
    val spillMap = new ExternalAppendOnlyMap[ByteKey, PayloadAndOffset, PayloadAndOffset]((v) => v, compactor, compactor)
    spillMap.insertAll(rawMessages)
    spillMap.iterator
  }

  protected def getPartitions: Array[Partition] = retry {

    //TODO use all ISRs not just leaders
    val leaders = client.getLeaders(topic)

    val startOffsets = client.topicOffsets(topic, sinceTimeMs, leaders).asScala
    val stopOffsets = client.topicOffsets(topic, KafkaClient.LATEST_TIME, leaders).asScala

    val offsets = startOffsets.map {
      case (partition, startOffset) => (partition, (startOffset, stopOffsets(partition)))
    }
    var index = -1
    leaders.asScala.flatMap { case (partition, leader) =>
      if (selectPartition >= 0 && selectPartition != partition) {
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

  protected def compute(split: Partition, context: TaskContext): Iterator[(ByteKey, PayloadAndOffset)] = {
    val kafkaSplit = split.asInstanceOf[KafkaSplit]
    val partition = kafkaSplit.partition
    val tap = new KafkaTopicAndPartition(topic, partition)
    val startOffset = kafkaSplit.startOffset
    val stopOffset = kafkaSplit.stopOffset

    def sleep() = Thread.sleep(client.refreshLeaderBackoffMs())

    try {
      // every task reads from a single broker
      // on the first attempt we use the lead broker determined in the driver, on next attempts we ask for the lead broker ourselves
      val broker = (if (context.attemptNumber == 0) kafkaSplit.leader else None)
        .orElse(client.getLeaders(topic).get(partition))
        .getOrElse(throw new RuntimeException(s"no leader for partition ${partition}"))

      val fetcher = client.connect(broker, tap)
      context.addTaskCompletionListener(_ => fetcher.close())

      fetcher.iterator(startOffset, stopOffset).asScala.map { keyPayloadAndOffset =>
        (keyPayloadAndOffset.key, keyPayloadAndOffset.payloadAndOffset)
      }

    } catch {
      case e: Exception => sleep(); throw e
//      case e: LeaderNotAvailableException => sleep(); throw e
//      case e: NotLeaderForPartitionException => sleep(); throw e
      case e: ConnectException => sleep(); throw e
    }
  }
}
