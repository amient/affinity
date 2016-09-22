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

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes.{MovedPermanently, NotFound, OK, SeeOther}
import akka.http.scaladsl.model.{ContentTypes, HttpResponse, Uri, headers}
import akka.pattern.ask
import akka.util.Timeout
import io.amient.affinity.example._
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.core.http.ResponseBuilder
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.UserInputMediator

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

trait Connect extends HttpGateway {

  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  abstract override def handle: Receive = super.handle orElse {

    /**
      * GET /vertex/<vertex>
      */
    case HTTP(GET, PATH("vertex", INT(id)), query, response) =>
      val task = cluster ? id
      fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
        case false => ResponseBuilder.json(NotFound, "Vertex not found" -> id)
        case props => ResponseBuilder.json(OK, props)
      }


    /**
      * POST /component/<vertex>/
      */
    case HTTP(POST, PATH("component", INT(id)), query, response) =>
      fulfillAndHandleErrors(response, collect(id), ContentTypes.`application/json`) {
        case false => ResponseBuilder.json(NotFound, "Vertex not found" -> id)
        case component => ResponseBuilder.json(OK, component)
      }

    /**
      * POST /connect/<vertex1>/<vertex2>
      */
    case HTTP(POST, PATH("connect", INT(id), INT(id2)), query, response) =>
      fulfillAndHandleErrors(response, connect(id, id2), ContentTypes.`application/json`) {
        case true => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/vertex/$id"))))
        case false => HttpResponse(MovedPermanently, headers = List(headers.Location(Uri(s"/vertex/$id"))))
      }

    /**
      * PUT /delegate
      */
    case HTTP(PUT, PATH("delegate"), query, response) =>
      val userInputMediator = service(classOf[UserInputMediator])
      userInputMediator ? "hello" onSuccess {
        case userInput: String => response.success(ResponseBuilder.json(OK, Map("userInput" -> userInput)))
      }

  }

  def connect(v1: Int, v2: Int): Future[Any] = {
    for {
      oneway <- (cluster ? ModifyGraph(v1, Edge(v2), GOP.ADD)).asInstanceOf[Future[Boolean]]
      if oneway
      reverse <- (cluster ? ModifyGraph(v2, Edge(v1), GOP.ADD)).asInstanceOf[Future[Boolean]]
    } yield reverse
  }

  def collect(vertex: Int): Future[Any] = {
    val promise = Promise[Component]()
    implicit val timeout = Timeout(10 seconds)
    def collect(v: Int, agg: Set[Int]): Unit = {
      promise.completeWith {
        cluster ? Component(v, agg) map {
          case Component(_, add) => Component(v, agg ++ add)
        }
      }
    }
    collect(vertex, Set())
    promise.future
  }


  //  /**
  //    * Connecting a Vertex to 1 or more other Vertices
  //    * This is done recursively and reliably using Ask instead of Tell.
  //    *
  //    * @return Future which fails if any of the propagation steps fail. When successful holds:
  //    *         true if the operation resulted in graph modification,
  //    *         false if there were no modifications and
  //    *         false if no changes were made.
  //    */
  //  private def connect(vertex: Vertex, withVertices: Set[Vertex], ts: Long = System.currentTimeMillis): Future[Boolean] = {
  //
  //    val promise = Promise[Boolean]()
  //
  //    implicit val timeout = Timeout(10 seconds)
  //
  //    cluster ? vertex map {
  //      case false => Set[Edge]()
  //      case props: VertexProps => props.edges
  //    } map { existingEdges: Set[Edge] =>
  //      val additionalEdges = withVertices.map(Edge(_, ts)).diff(existingEdges)
  //      if (additionalEdges.isEmpty) promise.success(false)
  //      else {
  //        cluster ? Component(vertex, additionalEdges, GOP.ADD) map {
  //          case false => promise.success(false)
  //          case true =>
  //            val propagate: List[Future[Boolean]] = existingEdges.toList.map { e =>
  //              connect(e.target, additionalEdges)
  //            } ++ additionalEdges.toList.map { e =>
  //              connect(e.target, Set(vertex))
  //            } map (_ map {
  //              case _ => true
  //            } recover {
  //              case t: Throwable => false
  //            })
  //
  //            for (successes <- Future.sequence(propagate)) {
  //              if (!successes.forall(_ == true)) {
  //                //TODO compensate failure - use atomic versioning on component object because this is all async and some other call may have modified the component
  //                //TODO cluster ! Component(vertex, additionalEdges, Component.RemoveEdges )
  //                //TODO AvroRecord enum mapping and evolution test by adding operation type to the Component
  //                //TODO to have a reliable compensation, each edge has to carry the correlation and root
  //                promise.failure(new IllegalStateException("propagation failed on existing component - attempting to compensate"))
  //              } else {
  //                promise.success(true)
  //              }
  //            }
  //        }
  //      }
  //    }
  //
  //    promise.future
  //  }


}
