/** Copyright (C) 2017-2018 Project-ODE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ode.perf

/**
 * Performance test for mutables-while vs immutables-zip-map
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMutablesWhileVsImmutablesZipMap
  extends PerfSpec[
    (Array[Double], Array[Double]),
    (Vector[Double], Vector[Double]),
    Seq[Double]]
  with ArraySizeSpec {

  // Trick to enforce matching floating-point numbers at start time
  val d1 = (
    (dataStart to dataEnd by dataStep).toVector.toArray,
    (dataStart to dataEnd by dataStep).toVector.toArray
  )
  val d2 = (
    (dataStart to dataEnd by dataStep).toVector,
    (dataStart to dataEnd by dataStep).toVector
  )
  val f1 = (t: (Array[Double], Array[Double])) => {
    var i: Int = 0
    val result = new Array[Double](t._1.length)
    while (i < t._1.length) {
      result(i) = 3.1415d * t._1(i) + t._2(i)
      i += 1
    }
    result.toSeq
  }
  val f2 = (t: (Vector[Double], Vector[Double])) => {
    t._1.zip(t._2).map(c => c._1 * 3.1415d + c._2).toSeq
  }
  val f1Desc = "mutables-while"
  val f2Desc = "immutables-zip-map"

}
