package io.amient.affinity.core.util

import java.util.concurrent.ConcurrentHashMap

import akka.http.scaladsl.model.HttpResponse
import com.codahale.metrics.MetricRegistry

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object AffinityMetrics extends MetricRegistry {

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  private val processMetricsMap = new ConcurrentHashMap[String, ProcessMetrics]()

  private def getProcessMetrics(name: String): ProcessMetrics = {
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
    val metrics = getProcessMetrics(groupName)
    val startTime = metrics.markStart()
    result.onComplete {
      case Success(response: HttpResponse) => if (response.status.intValue() < 400) metrics.markSuccess(startTime) else metrics.markFailure(startTime)
      case Success(_) => metrics.markSuccess(startTime)
      case Failure(_) => metrics.markFailure(startTime)
    }
  }

  class ProcessMetrics(name: String) {
    val inputCounter = counter(s"$name.counter")
    val processDurations = histogram(s"$name.durations")
    val successCounter = AffinityMetrics.counter(s"$name.successes")
    val failureCounter = AffinityMetrics.counter(s"$name.failures")

    def markStart(): Long = {
      inputCounter.inc()
      EventTime.unix
    }

    def markSuccess(startTime: Long): Unit = {
      processDurations.update(EventTime.unix - startTime)
      successCounter.inc()
    }

    def markFailure(startTime: Long): Unit = {
      processDurations.update(EventTime.unix - startTime)
      failureCounter.inc()
    }

  }

}
