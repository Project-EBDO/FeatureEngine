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
 * Performance test for array-copy vs manual-copy
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestArrayCopyVsManualCopy
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec {

  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray
  val f1 = (array: Array[Double]) => {
    val result2 = new Array[Double](array.length)
    Array.copy(array, 0, result2, 0, array.length)
    array
  }
  val f2 = (array: Array[Double]) => {
    val result = new Array[Double](array.length)
    var i: Int = 0
    while (i < array.length) {
      result(i) = array(i)
      i += 1
    }
    result
  }
  val f1Desc = "array-copy"
  val f2Desc = "manual-copy"

}
