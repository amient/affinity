package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{Actor, Props, Terminated}
import akka.event.Logging
import io.amient.akkahttp.{Coordinator, HttpInterface}

object Controller {

  final case class CreateRegion()

  final case class CreateGateway()

  final case class GatewayCreated()

}

class Controller(appConfig: Properties) extends Actor {

  import Controller._

  implicit val system = context.system

  val log = Logging.getLogger(context.system, this)

  //controller terminates the system so cannot use system.dispatcher for Futures execution
  import scala.concurrent.ExecutionContext.Implicits.global

  val coordinator = try {
    Coordinator.fromProperties(appConfig)
  } catch {
    case e: Throwable =>
      system.terminate() onComplete { _ =>
        e.printStackTrace()
        System.exit(10)
      }
      throw e
  }

  val httpInterface: Option[HttpInterface] = try {
    HttpInterface.fromConfig(appConfig)
  } catch {
    case e: Throwable =>
      system.terminate() onComplete { _ =>
        e.printStackTrace()
        System.exit(10)
      }
      throw e
  }

  override def receive: Receive = {

    case CreateRegion() =>
      try {
        context.actorOf(Props(new Region(appConfig, coordinator)), name = "region")
      } catch {
        case e: Throwable =>
          system.terminate() onComplete { _ =>
            e.printStackTrace()
            System.exit(11)
          }
      }

    case CreateGateway() =>
      try {
        context.watch(context.actorOf(Props(new Gateway(appConfig)), name = "gateway"))
      } catch {
        case e: Throwable =>
          system.terminate() onComplete { _ =>
            e.printStackTrace()
            System.exit(12)
          }
      }

    case GatewayCreated() =>
      try {
        coordinator.watchRoutees(context.system, sender)
        httpInterface.foreach(_.bind(sender))

      } catch {
        case e: Throwable =>
          system.terminate() onComplete { _ =>
            e.printStackTrace()
            System.exit(13)
          }
      }

    case Terminated(gateway) =>
      log.info("Terminated(gateway) shutting down http interface")
      try {
        httpInterface.foreach(_.close)
      } finally {
        system.terminate() onComplete { _ =>
          //TODO there still could be an error that caused the gateway to terminate
          System.exit(0)
        }
      }

    case any => system.terminate() onComplete { _ =>
      System.err.println("Unknown controller message " + any)
      System.exit(14)
    }
  }

}
