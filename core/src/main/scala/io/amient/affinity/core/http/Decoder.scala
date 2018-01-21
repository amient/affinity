package io.amient.affinity.core.http

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.HttpEntity
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import org.codehaus.jackson.{JsonFactory, JsonNode, JsonParser}
import org.codehaus.jackson.map.ObjectMapper

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Decoder {

  val mapper = new ObjectMapper()

  val factory = new JsonFactory()

  def jsonEntity(entity: HttpEntity)(implicit materializer: Materializer): Future[JsonNode] = {
    //TODO do this using pure streaming not by converting to blocking InputStream
    import materializer.executionContext
    Future {
      json(entity)
    }
  }

  def json(source: Source[ByteString, Any])(implicit materializer: Materializer): JsonNode = {
    val is = source.runWith(StreamConverters.asInputStream(FiniteDuration(3, TimeUnit.SECONDS)))
    val jp: JsonParser = factory.createJsonParser(is)
    mapper.readValue(jp, classOf[JsonNode])
  }

  def json(entity: HttpEntity)(implicit materializer: Materializer): JsonNode = {
    val source = entity.dataBytes
    json(source)
  }

  def json(content: String) = {
    mapper.readValue(content, classOf[JsonNode])
  }

}
