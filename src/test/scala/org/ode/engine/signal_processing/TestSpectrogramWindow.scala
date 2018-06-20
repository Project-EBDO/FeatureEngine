/** Copyright (C) 2017-2018 Project-ODE
s *
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
 * SpectrogramWindow test implementation
 * @param windowSize the window size for the test implementation
 *
 * @author Alexandre Degurse
 */
class TestWindow(val windowSize: Int) extends SpectrogramWindow {
  val windowCoefficients: Array[Double] =
    (1 to windowSize).map(_.toDouble).toArray
}

/**
 * Test of SpectrogramWindow test implementation
 *
 * @author Alexandre Degurse
 */
class TestSpectrogramWindow extends FlatSpec with Matchers {

  private val testWindowSize = 1024
  private val testWindow = new TestWindow(testWindowSize)
  private val testSignal = (1 to testWindowSize).map(1.0 / _.toDouble).toArray

  it should "fail applying with mutables when signal doesn't match window size" in {
    an [IllegalArgumentException] should be thrownBy testWindow.applyToSignal(testSignal.take(10))
  }

  it should "apply with mutables" in {
    testWindow.applyToSignal(testSignal).sum should equal(testWindowSize)
  }
}
