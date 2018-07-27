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

package io.amient.affinity

import java.lang

import io.amient.affinity.core.util.AckSupport

package object core extends AckSupport {
  def any2ref(any: Any): AnyRef = any match {
    case ref: AnyRef => ref
    case b: Byte => new lang.Byte(b)
    case c: Char => new lang.Character(c)
    case z: Boolean => new lang.Boolean(z)
    case s: Short => new lang.Short(s)
    case i: Int => new lang.Integer(i)
    case l: Long => new lang.Long(l)
    case f: Float => new lang.Float(f)
    case d: Double => new lang.Double(d)
  }
}
