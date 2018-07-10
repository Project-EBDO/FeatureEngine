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

package org.ode.engine.signal_processing.windowfunctions

import org.scalatest.{FlatSpec, Matchers}


/**
 * WindowFunction test implementation
 * @param windowSize the window size for the test implementation
 *
 * @author Alexandre Degurse
 */
class TestFunction(val windowSize: Int) extends WindowFunction {
  val windowCoefficients: Array[Double] = (1 to windowSize).map(_.toDouble).toArray
}

/**
 * Test of WindowFunction test implementation
 *
 * @author Joseph Allemandou, Alexandre Degurse
 */
class TestWindowFunction extends FlatSpec with Matchers {

  private val testWindowSize = 1024
  private val testWindow = new TestFunction(testWindowSize)
  private val testSignal = (1 to testWindowSize).map(1.0 / _.toDouble).toArray

  it should "raise an IllegalArgumentException when applying with mutables when signal doesn't match window size" in {
    the[IllegalArgumentException] thrownBy {
      testWindow.applyToSignal(testSignal.take(10))
    } should have message "Incorrect signal length (10) for SpectrogramWindow (1024)"
  }

  it should "apply with mutables" in {
    testWindow.applyToSignal(testSignal).sum should equal(testWindowSize)
  }
}
