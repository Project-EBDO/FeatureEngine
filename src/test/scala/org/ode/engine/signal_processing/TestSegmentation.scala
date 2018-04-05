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

package org.ode.engine.signal_processing

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for segmentation class
  * Author: Alexandre Degurse
  */


class TestSegmentation extends FlatSpec with Matchers {

  "Segmentation" should "compute a segmentated signal without overlap nor partial window" in {
    val signal: Array[Double] = (1.0 to 100.0 by 1.0).toArray
    val segmentationClass: Segmentation = new Segmentation(10)

    val expectedSegmentedSignal: Array[Array[Double]] = 
      (1.0 to 100.0 by 10.0)
        .map(x => (x to x + 9.0 by 1.0).toArray)
        .toArray 

    val segmentatedSignal = segmentationClass.compute(signal)

    segmentatedSignal.zip(expectedSegmentedSignal)
      .foreach(
        wins => wins._1 should be(wins._2)
      )
  }

  it should "compute a segmentated signal without overlap and drop partial window" in {
    val signal: Array[Double] = (1.0 to 110.0 by 1.0).toArray
    val segmentationClass: Segmentation = new Segmentation(25)

    val expectedSegmentedSignal: Array[Array[Double]] = 
      (1.0 to 100.0 by 25.0)
        .map(x => (x to x + 24.0 by 1.0).toArray)
        .toArray 

    val segmentatedSignal = segmentationClass.compute(signal)

    segmentatedSignal.zip(expectedSegmentedSignal)
      .foreach(
        wins => wins._1 should be(wins._2)
      )
  }
}
