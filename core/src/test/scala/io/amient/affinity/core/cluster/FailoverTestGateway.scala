package io.amient.affinity.core.cluster

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model.StatusCodes.{NotFound, OK, SeeOther}
import akka.http.scaladsl.model.{HttpResponse, Uri, headers}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.actor.GatewayHttp
import io.amient.affinity.core.cluster.FailoverTestPartition.{GetValue, PutValue}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.http.{Encoder, HttpInterfaceConf}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import io.amient.affinity.core.ack
import scala.language.postfixOps

class FailoverTestGateway extends GatewayHttp {

  override val rejectSuspendedHttpRequests = false

  override def listenerConfigs: Seq[HttpInterfaceConf] = List(HttpInterfaceConf(
    ConfigFactory.parseMap(Map("host" -> "127.0.0.1", "port" -> "0").asJava)))

  implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  implicit val scheduler = context.system.scheduler

  val keyspace1 = keyspace("keyspace1")

  override def handle: Receive = {
    case HTTP(GET, PATH(key), _, response) => handleWith(response) {
      implicit val timeout = Timeout(1 seconds)
      keyspace1 ?! GetValue(key) map {
        _ match {
          case None => HttpResponse(NotFound)
          case Some(value) => Encoder.json(OK, value, gzip = false)
        }
      }
    }

    case HTTP(POST, PATH(key, value), _, response) => handleWith(response) {
      implicit val timeout = Timeout(1 seconds)
      keyspace1 ?! PutValue(key, value) map {
        case _ => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/$key"))))
      }
    }
  }
}