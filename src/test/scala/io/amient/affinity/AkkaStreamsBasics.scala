///*
// * Copyright 2016 Michal Harish, michal.harish@gmail.com
// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.amient.affinity
//
//import akka.actor.ActorSystem
//import akka.stream.ActorMaterializer
//import akka.stream.scaladsl.{Flow, Sink, Source}
//
//import scala.io.StdIn
//
//
//object AkkaStreamsBasics extends App {
//
//  implicit val system = ActorSystem.create()
//
//  implicit val materializer = ActorMaterializer.create(system)
//
//  implicit val executionContext = system.dispatcher
//
//  val autoincrementSource = Source.fromIterator(() => {
//    var n = 0
//    Iterator.continually {
//      n += 1
//      n
//    }
//  })
//
//  val flowDoublingElements = Flow[Int].map(_ * 2)
//
//  autoincrementSource.via(flowDoublingElements).runWith(Sink.foreach[Int] { f =>
//    println(f)
//  })
//
//  StdIn.readLine()
//
//  system.terminate()
//}