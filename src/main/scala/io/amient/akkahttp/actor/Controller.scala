package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{Actor, Props, Terminated}
import akka.event.Logging
import io.amient.akkahttp.HttpInterface

object Controller {

  final case class CreateGateway()
  final case class GatewayCreated()
}

class Controller(appConfig: Properties) extends Actor {

  import Controller._

  implicit val system = context.system

  val log = Logging.getLogger(context.system, this)

  @volatile private var httpInterface: HttpInterface = null

  //controller terminates the system so cannot use system.dispatcher for Futures execution
  import scala.concurrent.ExecutionContext.Implicits.global

  override def receive: Receive = {

    case CreateGateway() =>
      try {
        context.watch(context.actorOf(Props(new Gateway(appConfig)), name = "gateway"))
      } catch {
        case e: Throwable =>
          system.terminate() onComplete { _ =>
              e.printStackTrace()
              System.exit(11)
          }
      }

    case GatewayCreated() =>
      try {
        httpInterface = new HttpInterface(appConfig, sender)
      } catch {
        case e: Throwable =>
          system.terminate() onComplete { _ =>
            e.printStackTrace()
            System.exit(12)
          }
      }

    case Terminated(gateway) =>
        if (httpInterface != null) {
          log.info("Terminated(gateway) shutting down http interface")
          try {
            httpInterface.close()
            httpInterface = null
          } finally {
            system.terminate() onComplete { _ =>
              //TODO there still could be an error that caused the gateway to terminate
              System.exit(0)
            }
          }
        } else {
          log.info("Terminated(gateway) no http interface")
          system.terminate() onComplete { _ =>
            System.err.println("Failed to construct Gateway, check logs for ActorInitializationException")
            System.exit(13)
          }
        }

    case any => system.terminate() onComplete { _ =>
      System.err.println("Unknown controller message " + any)
      System.exit(14)
    }
  }

}
