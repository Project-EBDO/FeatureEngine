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

package org.oceandataexplorer.engine.standardization

import java.io.File
import org.oceandataexplorer.engine.io.WavReader

import org.scalatest.{FlatSpec, Matchers}


/**
 * Class used to facilitate sound files reading through naming convention described in README.md
 *
 * @author Alexandre Degurse
 *
 * @param fileId - The sound identifier, unique among sounds
 * @param sysBits - The bytes number the file-writing system used (generally `64`, but could be `32`)
 * @param wavBits - The number of bits used for sound encoding (`8`, `16` or `24` generally)
 * @param samplingRate - The sampling rate the sound is using in kHz (usual values are `3.9`, `16.0`, `44.1`, `96.0`...)
 * @param chanNumber - The number of channels the file contains (`1` for mono, `2` for stereo etc)
 */

case class SoundHandler (
  fileId: String,
  sysBits: Int,
  wavBits: Int,
  sampleNumber: Int,
  samplingRate: Float,
  chanNumber: Int
) {

  private val soundPath: String = "/standardization/sounds"

  val soundParametersString: String = (fileId + "_"  + sysBits.toString
    + "_"  + wavBits.toString + "_" + sampleNumber.toString
    + "_"  + samplingRate.toString + "_" + chanNumber.toString
  )

  private val soundName: String = soundParametersString + ".wav"

  private val wavFile: File = new File(getClass.getResource(soundPath + "/" + soundName).toURI)

  private val wavReader = new WavReader(wavFile)

  def readSound(): Array[Double] = {
    wavReader
      .readChunks(sampleNumber, 0)(0)(0)
      .toArray
  }
}
