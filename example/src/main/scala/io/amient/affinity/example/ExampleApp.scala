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

package io.amient.affinity.example

import io.amient.affinity.example.partition.DataPartition
import io.amient.affinity.example.rest.HttpGateway
import io.amient.affinity.example.service.ServiceNode

object ExampleApp extends App {

  // singleton services
//  ServiceNode.main(Seq("2550").toArray)

  // gateways
  HttpGateway.main(Seq("8081").toArray)
//  HttpGateway.main(Seq("8082").toArray)

  // partition master(s)
  DataPartition.main(Seq("2551", "0,1").toArray)
  DataPartition.main(Seq("2552", "1,2").toArray)
  // partition standby(s)
  DataPartition.main(Seq("2553", "2,3").toArray)
  DataPartition.main(Seq("2554", "3,0").toArray)

}
