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
 * Performance test for mutables-while vs immutables-map
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMutablesWhileVsImmutablesMap
  extends PerfSpec[Array[Double], Vector[Double], Seq[Double]]
  with ArraySizeSpec {

  // Trick to enforce matching floating-point numbers at start time
  val d1 = (dataStart to dataEnd by dataStep).toVector.toArray
  val d2 = (dataStart to dataEnd by dataStep).toVector
  val f1 = (array: Array[Double]) => {
    var i: Int = 0
    val result = new Array[Double](array.length)
    while (i < array.length) {
      result(i) = 2.7182d * array(i) + 3.14159265d
      i += 1
    }
    result.toSeq
  }
  val f2 = (vector: Vector[Double]) => vector.map(v => 2.7182d * v + 3.14159265d).toSeq
  val f1Desc = "mutables-while"
  val f2Desc = "immutables-map"

}
