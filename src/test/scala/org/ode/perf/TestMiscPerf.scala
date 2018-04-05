/** Copyright (C) 2017 Project-ODE
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

import scala.math.{pow}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Misc performance tests
  * Author: Alexandre Degurse
  */


class TestMiscTests extends FlatSpec with Matchers {

  "TestMiscTests" should "show that x*x is 2 times faster than pow(x,2)" in {
    val t: Array[Double] = (0.0 to 100.0 by 0.1).toArray

    val tBeforePow = System.nanoTime()
    val sigPow: Array[Double] = t.map(x => pow(x, 2))
    val tAfterPow = System.nanoTime()
    val durationPow = (tAfterPow - tBeforePow).toDouble

    val tBeforeMult = System.nanoTime()
    val sigMult: Array[Double] = t.map(x => x*x)
    val tAfterMult = System.nanoTime()
    val durationMult = (tAfterMult - tBeforeMult).toDouble

    durationMult * 2.0 should be < durationPow
  }

  it should "show that using mutables with map is as fast as using immutables with map" in {
    val dataArray: Array[Double] = (0.0 to 100.0 by 0.1).toArray
    val dataVector: Vector[Double] = (0.0 to 100.0 by 0.1).toVector

    val tBefore1 = System.nanoTime()
    val result1 = dataArray.map(x => 2.7182*x + 3.14159265)
    val tAfter1 = System.nanoTime()
    val duration1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val result2 = dataVector.map(x => 2.7182*x + 3.14159265)
    val tAfter2 = System.nanoTime()
    val duration2 = (tAfter1 - tBefore1).toDouble

    duration1 should be(duration2)
  }

  it should "show that using mutables with while is as fast as using immutables with map" in {
    val dataArray: Array[Double] = (0.0 to 100.0 by 0.1).toArray
    val dataVector: Vector[Double] = (0.0 to 100.0 by 0.1).toVector

    val tBefore1 = System.nanoTime()
    var i: Int = 0
    val result1 = new Array[Double](dataArray.length) 
    while (i < dataArray.length) {
      result1(i) = 2.7182*dataArray(i) + 3.14159265
      i += 1
    }
    val tAfter1 = System.nanoTime()
    val duration1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val result2 = dataVector.map(x => 2.7182*x + 3.14159265)
    val tAfter2 = System.nanoTime()
    val duration2 = (tAfter1 - tBefore1).toDouble

    duration1 should be(duration2)
  }
}
