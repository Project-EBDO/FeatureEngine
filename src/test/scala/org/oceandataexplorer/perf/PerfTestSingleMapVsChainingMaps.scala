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

package org.oceandataexplorer.perf

/**
 * Performance test for single-map vs chaining-maps
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestSingleMapVsChainingMaps
  extends PerfSpec[Vector[Double], Vector[Double], Vector[Double]]
  with ArraySizeSpec {

  val d1 = (dataStart to dataEnd by dataStep).toVector
  val d2 = (dataStart to dataEnd by dataStep).toVector
  val f1 = (vector: Vector[Double]) => vector.map(v => {
    val nv = 2.7182 * v + 3.14159265
    2.0 * math.cos(nv * 3.0) + 5.0 * math.sin(nv * 0.5)
  })
  val f2 = (vector: Vector[Double]) =>
    vector
      .map(v => 2.7182 * v + 3.14159265)
      .map(v => 2.0 * math.cos(v * 3.0) + 5.0 * math.sin(v * 0.5))
  val f1Desc = "single-map"
  val f2Desc = "chaining-maps"

}
