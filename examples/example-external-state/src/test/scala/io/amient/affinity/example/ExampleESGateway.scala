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

package io.amient.affinity.example

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethods.GET
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, Partition}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH, QUERY}
import io.amient.affinity.core.util.Scatter

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

class ExampleESGateway extends GatewayHttp {

  val keyspace: ActorRef = keyspace("external")

  import context.dispatcher
  implicit val scheduler = context.system.scheduler
  override def handle: Receive = {

    case HTTP(GET, PATH("news", "latest"), QUERY(("w", watermark)), response) =>
      implicit val timeout = Timeout(5 seconds)
      val latestNews: Future[List[String]] = keyspace gather GetLatest(watermark.split(",").map(_.toLong))
      val responseText = latestNews.map(news=> "LATEST NEWS:\n" + news.mkString("\n"))
      handleAsText(response, responseText)
  }

}


case class GetLatest(watermark: Array[Long]) extends Scatter[List[String]] {
  override def gather(r1: List[String], r2: List[String]) = r1 ++ r2
}


class ExampleESPartition extends Partition {


  val marker = new LongAdder()
  val latest = new ConcurrentLinkedQueue[(String, String)]()

  state[String, String]("news").listen {
    case (k: String, Some(v: String)) =>
      latest.add((k, v))
      while (latest.size() > 3) latest.poll()
      //not a usual situation - actor logic synchronized over 2 threads
      // - but it's only so that the test is deterministic
      marker.synchronized {
        marker.increment
        marker.notify
      }
  }

  override def handle: Receive = {
    case request@GetLatest(watermark) => sender.reply(request) {
      while(marker.sum < watermark(partition)) {
        marker.synchronized {
          marker.wait(100)
        }
      }
      latest.iterator.asScala.map(_.productIterator.mkString(("\t"))).toList
    }
  }
}
