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

package org.oceandataexplorer.engine.signalprocessing

import org.oceandataexplorer.utils.test.OdeCustomMatchers
import org.scalatest.{FlatSpec, Matchers}


/**
 * Tests for Sound Calibration class
 *
 * @author Alexandre Degurse
 */
class TestSoundCalibration extends FlatSpec with Matchers with OdeCustomMatchers {

  val maxRMSE = 1.0E-16

  it should "calibrate a raw sound" in {
    val signal = Array(-1.1, 1.2, 1.3)
    val expectedCalibratedSignal = Array(-2.2, 2.4, 2.6)

    val calibrationFactor = 2.0
    val calibrationClass = new SoundCalibration(calibrationFactor)

    val calibratedSignal = calibrationClass.compute(signal)

    calibratedSignal should rmseMatch(expectedCalibratedSignal)
  }

  it should "raise an IllegalArgumentException when given a wrong calibrationFactor" in {
    the[IllegalArgumentException] thrownBy {
      new SoundCalibration(0.0)
    } should have message "Incorrect calibration factor (0.0) for SoundCalibration"

    the[IllegalArgumentException] thrownBy {
      new SoundCalibration(-1.0)
    } should have message "Incorrect calibration factor (-1.0) for SoundCalibration"
  }
}
