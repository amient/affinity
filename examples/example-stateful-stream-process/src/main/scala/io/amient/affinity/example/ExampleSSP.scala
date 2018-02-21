package io.amient.affinity.example

import io.amient.affinity.core.actor.GatewayStream

import scala.concurrent.Future

class ExampleSSP extends GatewayStream {

  val out = output[String, Long]("output-stream")

  val counter = global[String, Long]("state-counter")

  input[Null, String]("input-stream") {
    record =>
      val ack: Future[Unit] = counter.updateAndGet(record.value, current => current match {
        case None => Some(1)
        case Some(prev) => Some(prev + 1)
      }).collect {
        case Some(updatedCount) => out.write(Iterator.single((record.value, updatedCount)))
      }(scala.concurrent.ExecutionContext.Implicits.global)

      //TODO #144 add guarantees - at the moment
  }

}
