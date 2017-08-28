package io.amient.affinity.core.http

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.HttpEntity
import akka.stream.Materializer
import akka.stream.scaladsl.StreamConverters
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Decoder {

  val mapper = new ObjectMapper()

  def jsonEntity(entity: HttpEntity)(implicit materializer: Materializer): Future[JsonNode] = {
    //TODO do this using pure streaming not by converting to blocking InputStream
    import materializer.executionContext
    Future {
      json(entity)
    }
  }

  def json(entity: HttpEntity)(implicit materializer: Materializer): JsonNode = {
    val is = entity.dataBytes.runWith(StreamConverters.asInputStream(FiniteDuration(3, TimeUnit.SECONDS)))
    val jp: JsonParser = mapper.getFactory.createParser(is)
    mapper.readValue(jp, classOf[JsonNode])
  }

  def json(content: String) = {
    mapper.readValue(content, classOf[JsonNode])
  }

}
