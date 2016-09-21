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
import io.amient.affinity.example.{Component, Vertex}
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

    case HTTP(POST, PATH("com", INT(id), INT(id2)), query, response) =>
      fulfillAndHandleErrors(response, connect(Vertex(id), Set(Vertex(id2))), ContentTypes.`application/json`) {
        case true => HttpResponse(SeeOther, headers = List(headers.Location(Uri(s"/com/$id"))))
        case false => HttpResponse(MovedPermanently, headers = List(headers.Location(Uri(s"/com/$id"))))
      }

    case HTTP(GET, PATH("com", INT(id)), query, response) =>
      val task = cluster ? Vertex(id)
      fulfillAndHandleErrors(response, task, ContentTypes.`application/json`) {
        case false => ResponseBuilder.json(NotFound, "Vertex not found" -> id)
        case component => ResponseBuilder.json(OK, component)
      }

    case HTTP(GET, PATH("delegate"), query, response) =>
      val userInputMediator = service(classOf[UserInputMediator])
      userInputMediator ? "hello" onSuccess {
        case userInput: String => response.success(ResponseBuilder.json(OK, Map("userInput" -> userInput)))
      }

  }


  /**
    * Connecting a Vertex to 1 or more other Vertices
    * This is done recursively and reliably using Ask instead of Tell.
    *
    * @return Future which fails if any of the propagation steps fail. When successful holds:
    *         true if the operation resulted in graph modification,
    *         false if there were no modifications and
    *         false if no changes were made.
    */
  private def connect(vertex: Vertex, withEdges: Set[Vertex]): Future[Boolean] = {

    val promise = Promise[Boolean]()

    implicit val timeout = Timeout(10 seconds)

    cluster ? vertex map {
      case false => Set[Vertex]()
      case component: Component => component.edges
    } map { existingEdges =>
      val additionalEdges = withEdges.diff(existingEdges)
      if (additionalEdges.isEmpty) promise.success(false)
      else {
        cluster ? Component(vertex, additionalEdges /*Component.AddEdges*/) map {
          case false => promise.success(false)
          case true =>
            val propagate: List[Future[Boolean]] = existingEdges.toList.map {
              connect(_, additionalEdges)
            } ++ additionalEdges.toList.map {
              connect(_, Set(vertex))
            } map (_ map {
              case _ => true
            } recover {
              case t: Throwable => false
            })

            for (successes <- Future.sequence(propagate)) {
              if (!successes.forall(_ == true)) {
                //TODO compensate failure - use atomic versioning on component object because this is all async and some other call may have modified the component
                //TODO cluster ! Component(vertex, additionalEdges, Component.RemoveEdges )
                //TODO AvroRecord enum mapping and evolution test by adding operation type to the Component
                promise.failure(new IllegalStateException("propagation failed on existing component - attempting to compensate"))
              } else {
                promise.success(true)
              }
            }
        }
      }
    }

    promise.future
  }


}
