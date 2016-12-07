/**
  * HttpRDD
  * Copyright (C) 2015 Michal Harish
  * <p/>
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  * <p/>
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  * <p/>
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package io.amient.util.spark

import java.util

import io.amient.util.HttpData
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

case class HttpRDD[X](rdd: RDD[X], port: Int = 0) extends HttpData(rdd.name, port) {
  override protected def open(): util.Iterator[_] = {
    rdd.toLocalIterator.map(convert2csv(_)).asJava
  }

  def convert2csv(x: Any): String = x match {
    case p: Product => convert2csv(p.productIterator)
    case i: scala.collection.Iterable[_] => i.foldLeft("")((a,b) => (if (a.isEmpty) "" else a + ",") + convert2csv(b))
    case i: scala.collection.Iterator[_] => i.foldLeft("")((a,b) => (if (a.isEmpty) "" else a + ",") + convert2csv(b))
    case _: Any => x.toString
  }
}