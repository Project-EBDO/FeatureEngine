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

package org.ode.engine.signal_processing

/**
 * Class computing Third-Octave Level over a Power Spectrum.
 *
 * Some abbreviations are used here:
 * - TO: third octave, used to designate implicitely the center frequency of a TO band
 * - TOB: third octave band, designates the band of frequency of a TO
 * - TOL: third octave level, designates the Sound Pressure Level of a TOB
 *
 * @author Paul Nguyen HD, Alexandre Degurse, Joseph Allemandou
 *
 * @param nfft The size of the fft-computation window
 * @param samplingRate The sampling rate of the signal
 * @param lowFreq The low boundary of the frequency range to study
 * @param highFreq The high boundary of the frequency range to study
 */


class TOL
(
  val nfft: Int,
  val samplingRate: Float,
  val lowFreq: Option[Double] = None,
  val highFreq: Option[Double] = None
) extends FrequencyConvertible {

  if (nfft < samplingRate) {
    throw new IllegalArgumentException(
      s"Incorrect window size ($nfft) for TOL ($samplingRate)"
    )
  }

  // we're using acoustic constants
  // scalastyle:off magic.number

  // the first accepted TO is the 0th, center at 1.0 Hz
  private val lowerLimit = 1.0
  private val upperLimit = math.min(samplingRate / 2.0, math.pow(10, 0.1 * 60) * math.pow(10, 0.05))

  if (lowFreq.filter(lf => lf < lowerLimit || lf > highFreq.getOrElse(upperLimit)).isDefined) {
    throw new IllegalArgumentException(
      s"Incorrect low frequency (${lowFreq.get}) for TOL "
      + s"(smaller than $lowerLimit or bigger than ${highFreq.getOrElse(upperLimit)})"
    )
  }

  if (highFreq.filter(hf => hf > upperLimit || hf < lowFreq.getOrElse(lowerLimit)).isDefined) {
    throw new IllegalArgumentException(
      s"Incorrect high frequency (${highFreq.get}) for TOL "
      + s"(higher than $upperLimit or smaller than ${lowFreq.getOrElse(lowerLimit)})"
    )
  }

  private val tocScalingFactor = math.pow(10, 0.05)

  /**
   * Generate third octave band boundaries for each center frequency of third octave
   */
  val thirdOctaveBandBounds: Array[(Double, Double)] = {
    // See https://en.wikipedia.org/wiki/Octave_band#Base_10_calculation
    (0 to 60)
      // convert third octaves indicies to the frequency of the center of their band
      .map(toIndex => math.pow(10, (0.1 * toIndex)))
      // scalastyle:on magic.number
      // convert center frequency to a tuple of (lowerBoundFrequency, upperBoundFrequency)
      .map(toCenter => (toCenter / tocScalingFactor, toCenter * tocScalingFactor))
      // keep only the band within the study range
      .filter{tob =>
        // partial bands are kept
        (tob._2 >= lowFreq.getOrElse(lowerLimit) && tob._2 <= highFreq.getOrElse(upperLimit))
      }
      .toArray
  }

  // Compute the indices associated with each TOB boundary
  private val boundIndicies: Array[(Int, Int)] = thirdOctaveBandBounds.map(
    bound => (frequencyToSpectrumIndex(bound._1), frequencyToSpectrumIndex(bound._2))
  )

  /**
   * Function computing the Third Octave Levels over a PSD
   *
   * Default environmentals parameters ensures there is no correction on
   * on the third-octave levels.
   *
   * @param spectrum The one-sided Power Spectral Density
   * as an Array[Double] of length spectrumSize
   * TOL can be computed over a periodogram, although, functionnaly, it makes more sense
   * to compute it over a Welch estimate of PSD
   * @param vADC The voltage of Analog Digital Converter used in the microphone (given in volts)
   * @param microSensitivity Microphone sensitivity (without gain, given in dB)
   * @param gain Gain used for the hydrophones (given in dB)
   * @return The Third Octave Levels over the PSD as a Array[Double]
   */
  def compute
  (
    spectrum: Array[Double],
    vADC: Double = 1.0,
    microSensitivity: Double = 0.0,
    gain: Double = 0.0
  ): Array[Double] = {

    if (spectrum.length != spectrumSize) {
      throw new IllegalArgumentException(
        s"Incorrect PSD size (${spectrum.length}) for TOL ($spectrumSize)"
      )
    }

    val logNormalization: Double = microSensitivity + gain + 20*math.log10(1.0/vADC)
    val tols = new Array[Double](boundIndicies.length)

    // scalastyle:off while var.local
    var i = 0
    var j = 0
    var tol: Double = 0.0

    while (i < boundIndicies.length) {
      j = boundIndicies(i)._1
      while (j < boundIndicies(i)._2) {
        tol += spectrum(j)
        j += 1
      }
      tols(i) = 10.0 * math.log10(tol) - logNormalization
      tol = 0.0
      i += 1
    }
    // scalastyle:on while var.local
    tols
  }
}
