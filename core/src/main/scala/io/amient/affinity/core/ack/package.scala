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

package io.amient.affinity.core

import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

package object ack {

  def ack(ref: ActorRef, message: Any)(implicit context: ExecutionContext): Future[Any] = {
    val promise = Promise[Any]()
    def retry(num: Int): Unit = {
      implicit val timetout = Timeout(1 second)
      ref ? message onComplete {
        case Success(result) => promise.success(result)
        case Failure(cause) => {
          cause match {
            case to: AskTimeoutException if (to.getCause() == null) => promise.failure(cause)
            case _ => if (num == 0) promise.failure(cause) else {
              System.out.println(s"$ref failed to ack message $message, " + cause.getMessage)
              retry(num - 1)
            }
          }
        }
      }
    }
    retry(5)
    promise.future
  }
}
