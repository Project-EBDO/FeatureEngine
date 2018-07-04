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

package org.ode.engine.standardization

import java.io.{File, FileInputStream, InputStream}
import scala.io.Source

import org.ode.utils.test.ErrorMetrics.rmse
import org.ode.engine.signal_processing._

import org.scalatest.{FlatSpec, Matchers}

/**
  * Class used to read result files that correspond to a certain specification
  *
  * Author: Alexandre Degurse
  */

case class ResultFileParameters(
  sound: SoundParameters,
  algo: String,
  nfft: Int,
  winSize: Int,
  offset: Int,
  vSysBits: Int,
  location: String
) {

  val maxRMSE = 1.0E-16

  val paramsString = (sound.soundParametersString
    + "_" + algo + "_" + nfft.toString + "_" + winSize.toString
    + "_" + offset.toString + "_" +vSysBits.toString
  )

  val fileName = paramsString + ".csv"
  println(fileName)
  val file = new File(getClass.getResource(location + "/" + fileName).toURI)

  // instanciate the most generic signal_processing classes
  val segmentation = new Segmentation(winSize, Some(offset))

  val hammingSymmetric = new HammingWindow(winSize, "symmetric")
  val hammingPeriodic = new HammingWindow(winSize, "periodic")
  val hammingScalingSymmetric = hammingSymmetric.normalizationFactor(1.0)

  val fftClass = new FFT(nfft)
  val periodogramClassNormInDensity = new Periodogram(nfft, 1 / (sound.samplingRate * hammingScalingSymmetric))
  val welchClass = new WelchSpectralDensity(nfft, sound.samplingRate.toFloat)

  val tolClass = if (winSize >= sound.samplingRate) {
    new TOL(nfft, sound.samplingRate.toFloat, Some(0.2 * sound.samplingRate), Some(0.4 * sound.samplingRate))
  } else null

  def computeAlgo(): Array[Array[Double]] = algo match {
    case "vFFT" => {
      val signal = sound.readSound()
      val norm = 1.0 / math.sqrt(math.pow(hammingSymmetric.windowCoefficients.sum, 2))

      segmentation.compute(signal)
        .map(segment => hammingSymmetric.applyToSignal(segment))
        .map(win => fftClass.compute(win))
        .map(fft => fft.map(_ * norm))
    }

    case "vPSD" => {
      val signal = sound.readSound()

      segmentation.compute(signal)
        .map(segment => hammingSymmetric.applyToSignal(segment))
        .map(win => fftClass.compute(win))
        .map(fft => periodogramClassNormInDensity.compute(fft))
    }

    case "vWelch" => {
      val signal = sound.readSound()

      val periodograms = segmentation.compute(signal)
        .map(segment => hammingSymmetric.applyToSignal(segment))
        .map(win => fftClass.compute(win))
        .map(fft => periodogramClassNormInDensity.compute(fft))

      Array(welchClass.compute(periodograms))
    }

    case "vTOL" =>  {
      val signal = sound.readSound()

      val periodograms = segmentation.compute(signal)
        .map(segment => hammingSymmetric.applyToSignal(segment))
        .map(win => fftClass.compute(win))
        .map(fft => periodogramClassNormInDensity.compute(fft))

      val welch = welchClass.compute(periodograms)

      Array(tolClass.compute(welch))
    }

    case "fWelch" => Array(welchClass.frequencyVector())
  }

  def getExpectedValues(): Array[Array[Double]] = ResultFileParameters.readResultFile(file)
}

object ResultFileParameters {
  /**
   * Function that reads a file and extract its values as Double.
   * The first dimension corresponds the line in the file.
   * The second dimension corresponds to the Array contained in a line of the File.
   * If the values in the file are contained in columns use .transpose on the Array[Array[Double]]
   * to get the values in the right order.
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
