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

import scala.math._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Misc performance tests
  * Author: Alexandre Degurse
  */


class TestMiscPerf extends FlatSpec with Matchers {

  val dataStart = 0.0
  val dataEnd = 10000.0
  val dataStep = 0.1

  "TestMiscPerf" should "show that x*x is faster than pow(x,2)" in {
    var tBeforeMult = 0L
    var tBeforePow = 0L
    var tAfterMult = 0L
    var tAfterPow = 0L
    var durationMult = 0.0
    var durationPow = 0.0
    var ratio = 0.0
    
    val t: Array[Double] = (0.0 to 100.0 by 0.1).toArray

    for (i <- (1 to 500)) {

      tBeforePow = System.nanoTime()
      val sigPow: Array[Double] = t.map(x => pow(x, 2))
      tAfterPow = System.nanoTime()
      durationPow = (tAfterPow - tBeforePow).toDouble

      tBeforeMult = System.nanoTime()
      val sigMult: Array[Double] = t.map(x => x*x)
      tAfterMult = System.nanoTime()
      durationMult = (tAfterMult - tBeforeMult).toDouble

      ratio += (durationPow / durationMult)


    }
    ratio /= 500

    ratio should be > (1.0)
  }

  it should "show that using mutables with map is as fast as using immutables with map" in {
    val dataArray: Array[Double] = (0.0 to dataEnd by dataStep).toArray
    val dataVector: Vector[Double] = (0.0 to dataEnd by dataStep).toVector

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
    val dataArray: Array[Double] = (dataStart to dataEnd by dataStep).toArray
    val dataVector: Vector[Double] = (dataStart to dataEnd by dataStep).toVector

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

  it should "show that using mutables with while as fast as using immutables when chaining maps" in {
    val dataArray: Array[Double] = (dataStart to dataEnd by dataStep).toArray
    val dataVector: Vector[Double] = (dataStart to dataEnd by dataStep).toVector

    val tBefore1 = System.nanoTime()
    val result1 = new Array[Double](dataArray.length) 
    var i: Int = 0
    while (i < dataArray.length) {
      result1(i) = 2.0 * cos(3.0*(2.7182*dataArray(i) + 3.14159265)) + 5.0*sin(0.5*(2.7182*dataArray(i) + 3.14159265))
      i += 1
    }
    val tAfter1 = System.nanoTime()
    val duration1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val result2 = dataVector
      .map(x => 2.7182*x + 3.14159265)
      .map(x => 2.0*cos(x*3.0) + 5.0 * sin(0.5*x))
    val tAfter2 = System.nanoTime()
    val duration2 = (tAfter1 - tBefore1).toDouble

    duration1 should be(duration2)
  }

  it should "show that using mutables with while is faster than using immutables when using zip" in {
    val timeArray: Array[Double] = (dataStart to dataEnd by dataStep).toArray
    val phaseArray: Array[Double] = (dataStart to dataEnd by dataStep).toArray
    val timeVector: Vector[Double] = (dataStart to dataEnd by dataStep).toVector
    val phaseVector: Vector[Double] = (dataStart to dataEnd by dataStep).toVector

    val tBefore1 = System.nanoTime()
    val result1 = new Array[Double](timeArray.length) 
    var i: Int = 0
    while (i < timeArray.length) {
      result1(i) = cos(3.1415 * timeArray(i) + phaseArray(i))
      i += 1
    }
    val tAfter1 = System.nanoTime()
    val duration1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val result2 = timeVector
      .zip(phaseVector)
      .map(c => cos(c._1*3.1415 + c._2))
    val tAfter2 = System.nanoTime()
    val duration2 = (tAfter1 - tBefore1).toDouble

    duration1 should be(duration2)
  }

  it should "show that using Array.copy is as fast as manual copy" in {
    val data: Array[Double] = (dataStart to dataEnd by dataStep).toArray

    val tBefore1 = System.nanoTime()
    val result1 = new Array[Double](data.length) 
    var i: Int = 0
    while (i < data.length) {
      result1(i) = data(i)
      i += 1
    }
    val tAfter1 = System.nanoTime()
    val duration1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val result2 = new Array[Double](data.length) 
    Array.copy(data, 0, result2, 0, data.length)
    val tAfter2 = System.nanoTime()
    val duration2 = (tAfter1 - tBefore1).toDouble

    duration1 should be(duration2)
  }
}
