/*
 * Copyright 2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.util

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import com.codahale.metrics.{JmxReporter, MetricRegistry}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object AffinityMetrics {
  type Reporter = MetricRegistry => JmxReporter
  private val reporters = scala.collection.mutable.ListBuffer[Reporter]()

  def addReporter(f: Reporter): Unit = reporters += f

  private val metricsRegistries = new ConcurrentHashMap[ActorSystem, AffinityMetrics]()

  def forActorSystem(system: ActorSystem): AffinityMetrics = {
    metricsRegistries.get(system) match {
      case null =>
        val registry = new AffinityMetrics
        reporters.foreach(_(registry).start)
        metricsRegistries.put(system, registry)
        registry
      case registry => registry
    }
  }
}

class AffinityMetrics extends MetricRegistry {

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  private val processMetricsMap = new ConcurrentHashMap[String, ProcessMetrics]()

  def meterAndHistogram(name: String): ProcessMetrics = {
    processMetricsMap.get(name)  match {
      case null =>
        val m = new ProcessMetrics(name)
        processMetricsMap.put(name, m)
        m
      case some => some
    }
  }

  def process(groupName: String, result: Promise[_]): Unit = process(groupName, result.future)

  def process(groupName: String, result: Future[Any]): Unit = {
    val metrics = meterAndHistogram(groupName)
    val startTime = metrics.markStart()
    result.onComplete {
      case Success(response: HttpResponse) => if (response.status.intValue() < 400) metrics.markSuccess(startTime) else metrics.markFailure(startTime)
      case Success(_) => metrics.markSuccess(startTime)
      case Failure(_) => metrics.markFailure(startTime)
    }
  }

  class ProcessMetrics(name: String) {
    val rate = meter(s"$name.rate")
    val durations = histogram(s"$name.durations")
    val successes = meter(s"$name.successes")
    val failures = meter(s"$name.failures")

    def markStart(): Long = {
      rate.mark()
      EventTime.unix
    }

    def markSuccess(startTime: Long, n: Long = 1): Unit = {
      durations.update(EventTime.unix - startTime)
      successes.mark(n)
    }

    def markFailure(startTime: Long): Unit = {
      durations.update(EventTime.unix - startTime)
      failures.mark()
    }

  }

}
