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

package org.ode.engine.standardization

import java.io.{File, FileInputStream, InputStream}
import scala.io.Source

import org.ode.engine.signal_processing._

import org.scalatest.{FlatSpec, Matchers}

/**
 * Class used to read result files with the filename format describe in README.md
 *
 * @author Alexandre Degurse
 */

case class ResultsHandler (
  sound: SoundHandler,
  algo: String,
  nfft: Int,
  winSize: Int,
  offset: Int,
  vSysBits: Int,
  location: String
) {

  private val maxRMSE = 1.0E-16

  val paramsString = (sound.soundParametersString
    + "_" + algo + "_" + nfft.toString + "_" + winSize.toString
    + "_" + offset.toString + "_" + vSysBits.toString
  )

  val fileName = paramsString + ".csv"
  private val file = new File(getClass.getResource(location + "/" + fileName).toURI)

  // instanciate the signal_processing classes
  private val segmentation = new Segmentation(winSize, Some(offset))

  private val hammingSymmetric = new HammingWindow(winSize, "symmetric")
  private val hammingPeriodic = new HammingWindow(winSize, "periodic")
  private val hammingScalingSymmetric = hammingSymmetric.normalizationFactor(1.0)

  private val fftClass = new FFT(nfft, sound.samplingRate)
  private val periodogramClassNormInDensity = new Periodogram(
    nfft,
    1 / (sound.samplingRate.toDouble * hammingScalingSymmetric),
    sound.samplingRate
  )
  private val welchClass = new WelchSpectralDensity(nfft, sound.samplingRate)

  /**
   * We're using a convention for TOL, the study frequency range is conventionnaly [0.2 * samplingRate, 0.4 * samplingRate]
   */
  private val tolClass = if (winSize >= sound.samplingRate) {
    new TOL(nfft, sound.samplingRate, Some(0.2 * sound.samplingRate.toDouble), Some(0.4 * sound.samplingRate.toDouble))
  } else null

  /**
   * Function computing results given the class paramters
   * @return The results of the computation defined by the class parameters as a Array of Array[Double]
   */
  def getComputedValues(): Array[Array[Double]] = {
    val signal = sound.readSound()

    val ffts = segmentation.compute(signal)
      .map(segment => hammingSymmetric.applyToSignal(segment))
      .map(win => fftClass.compute(win))

    algo match {
      case "vFFT" => {
        // Scipy Short Time Fourier Transform normalizes the ffts using the
        // sqrt of the normalizationFactor used to normalize Power Spectrum.
        val norm = 1.0 / math.sqrt(math.pow(hammingSymmetric.windowCoefficients.sum, 2))

        ffts.map(fft => fft.map(_ * norm))
      }

      case "vPSD" => {
        ffts
          .map(fft => periodogramClassNormInDensity.compute(fft))
      }

      case "vWelch" => {
        val periodograms = ffts
          .map(fft => periodogramClassNormInDensity.compute(fft))

        Array(welchClass.compute(periodograms))
      }

      case "vTOL" =>  {
        val welch = welchClass.compute(
          ffts.map(fft => periodogramClassNormInDensity.compute(fft))
        )

        Array(tolClass.compute(welch))
      }

      case "fWelch" => Array(welchClass.frequencyVector)
    }
  }

  /**
   * Function used to read the result file described by the class parameters
   * @return The expected values for the class parameters as a Array of Array[Double]
   */
  def getExpectedValues(): Array[Array[Double]] = ResultsHandler.readResultFile(file)
}

object ResultsHandler {
  /**
   * Function that reads a file and extract its values as Double.
   * The first dimension corresponds the lines in the file.
   * The second dimension corresponds the content of a line.
   *
   * @param resultFile The file to be read as a java.io.File
   * @return The values contained in the file as a Array[Array[Double]] order as described above.
   */
  def readResultFile(resultFile: File): Array[Array[Double]] = {
    val resultFileInputStream: InputStream = new FileInputStream(resultFile)
    val resultString = Source.fromInputStream(resultFileInputStream).mkString.trim.split("\n")

    resultString
      .map{s =>
        s.trim
          .split(" ")
          .map(x => x.toDouble)
      }
      .toArray
  }
}
