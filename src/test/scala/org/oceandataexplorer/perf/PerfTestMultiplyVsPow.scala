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
 * Performance test for multiplication vs math.pow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMultiplyVsPow
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec {

  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray
  val f1 = (array: Array[Double]) => array.map(v => v * v)
  val f2 = (array: Array[Double]) => array.map(v => math.pow(v, 2))
  val f1Desc = "simple-self-multiplication"
  val f2Desc = "math.pow(2)"

}
