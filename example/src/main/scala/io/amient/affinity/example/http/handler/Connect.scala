/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.example.rest.handler

import java.util.concurrent.ConcurrentHashMap

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.{MovedPermanently, NotFound, OK, SeeOther}
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.core.ack._
import io.amient.affinity.core.http.Encoder
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.example._
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.UserInputMediator

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait Connect extends HttpGateway {

  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  val cache = new ConcurrentHashMap[Int, HttpResponse]()

  abstract override def handle: Receive = super.handle orElse {

    /**
      * GET /vertex/<vertex>
      */
    case HTTP(GET, PATH("vertex", INT(id)), query, response) =>
      delegateAndHandleErrors(response, ack(cluster, GetVertexProps(id))) {
        Encoder.json(OK, _)
      }

    /**
      * GET /component/<vertex>/
      */
    case HTTP(POST, PATH("component", INT(id)), query, response) =>

      cache.get(id) match {
        case cached if (cached != null && cached.header[headers.Date].get.date.plus(5000) > DateTime.now) =>
          response.success(cached)

        case _ =>
          delegateAndHandleErrors(response, collectComponent(id)) {
            case false => Encoder.json(NotFound, "Vertex not found" -> id)
            case component: Component =>
              val response = Encoder.json(OK, component)
              cache.put(id, response)
              response
          }
      }


    /**
      * POST /connect/<vertex1>/<vertex2>
      */
    case HTTP(POST, PATH("connect", INT(id), INT(id2)), query, response) =>
      delegateAndHandleErrors(response, connect(id, id2)) {
        case true => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/vertex/$id"))))
        case false => HttpResponse(MovedPermanently, headers = List(headers.Location(Uri(s"/vertex/$id"))))
      }

    /**
      * POST /disconnect/<vertex1>/<vertex2>
      */
    case HTTP(POST, PATH("disconnect", INT(id), INT(id2)), query, response) =>
      delegateAndHandleErrors(response, disconnect(id, id2)) {
        case true => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/vertex/$id"))))
        case false => HttpResponse(MovedPermanently, headers = List(headers.Location(Uri(s"/vertex/$id"))))
      }

    /**
      * PUT /delegate
      */
    case HTTP(PUT, PATH("delegate"), query, response) =>
      val userInputMediator = service(classOf[UserInputMediator])
      userInputMediator ? "hello" onSuccess {
        case userInput: String => response.success(Encoder.json(OK, Map("userInput" -> userInput)))
      }

  }

  private def collectComponent(vertex: Int): Future[Component] = {
    val promise = Promise[Component]()
    val ts = System.currentTimeMillis
    implicit val timeout = Timeout(10 seconds)
    def collect(queue: Set[Int], agg: Set[Int]): Unit = {
      if (queue.isEmpty) promise.success(Component(vertex, ts, agg))
      else ack(cluster, CollectComponent(queue.head)) map {
        case Component(_, _, add) => collect(queue.tail ++ (add -- agg), agg ++ add)
      } recover {
        case NonFatal(e) => promise.failure(e)
      }
    }
    collect(Set(vertex), Set(vertex))
    promise.future
  }

  private def connect(v1: Int, v2: Int): Future[Boolean] = modify(v1, v2, GOP.ADD)

  private def disconnect(v1: Int, v2: Int): Future[Boolean] = modify(v1, v2, GOP.REMOVE)

  private def modify(v1: Int, v2: Int, op: GOP.Value): Future[Boolean] = {

    //to handle fatal crash consistency we'd need something like def WAL(t: java.lang.Long = System.currentTimeMillis) = op -> t

    val ts = System.currentTimeMillis
    val promise = Promise[Boolean]()
    val m1 = ModifyGraph(v1, Edge(v2, ts), op)
    ack(cluster, m1) onComplete {
      case Failure(e) => promise.failure(e) //earliest possible failure, nothing to rollback just report failure
      case Success(false) => promise.success(false)
      case Success(_) =>
        val m2 = ModifyGraph(v2, Edge(v1, ts), op)
        ack(cluster, m2) onComplete {
          case Failure(e) =>
            //second failure case we have to rollback the first edge modification
            cluster ! m1.inverse
            promise.failure(e)
          case Success(false) => promise.failure(new IllegalStateException)
          case Success(_) =>
            Future.sequence(op match {
              case GOP.ADD => List(collectComponent(v2))
              case GOP.REMOVE => List(collectComponent(v1), collectComponent(v2))
            }) onComplete {
              case Failure(e) =>
                //third failure case we have to rollback both edge modifications
                cluster ! m1.inverse
                cluster ! m2.inverse
                promise.failure(e)
              case Success(components) =>
                Future.sequence(components.map { component =>
                  Future.sequence(component.component.toSeq.map { v =>
                    ack(cluster, UpdateComponent(v, component.component)) recover {
                      case e: Throwable => null.asInstanceOf[Component]
                    }
                  })
                }) map { componentUpdates =>
                  if (componentUpdates.forall(_.forall(_ != null))) {
                    //all successful
                    promise.success(true)
                  } else {
                    promise.failure(new UnknownError)
                    // the worst possible failure, rollback everything that was modified
                    cluster ! m1.inverse
                    cluster ! m2.inverse
                    componentUpdates.foreach {
                      _ filter (_ != null) foreach { rollbackComponent =>
                        cluster ! rollbackComponent
                      }
                    }
                  }
                }
            }
        }
    }
    promise.future
  }

}
