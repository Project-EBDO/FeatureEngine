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


/**
 * Class calibrating raw acoustic signal with a calibration factor
 *
 * @author Alexandre Degurse
 *
 * @param calibrationFactor The calibration factor for raw sound calibration
 */
case class SoundCalibration(calibrationFactor: Double) extends Serializable {
  if (calibrationFactor <= 0.0) {
    throw new IllegalArgumentException(
      s"Incorrect calibration factor ($calibrationFactor) for SoundCalibration"
    )
  }

  /**
   * Method calibrating raw acoustic signal by using a calibration factor
   *
   * @param rawSignal The signal to be calibrated as a Array[Double]
   * @return The calibrated signal as a Array[Double]
   */
  def compute(rawSignal: Array[Double]) : Array[Double] = rawSignal.map(_ * calibrationFactor)
}
