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
 * Class that computes Third-Octave Level over a Power Spectral Density.
 *
 * Some abbreviations are used here:
 * - TO: third octave, used to designate implicitly is the frequency of TO band
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
  val samplingRate: Double,
  val lowFreq: Option[Double] = None,
  val highFreq: Option[Double] = None
) {

  if (nfft < samplingRate) {
    throw new IllegalArgumentException(
      s"Incorrect window size ($nfft) for TOL ($samplingRate)"
    )
  }

  if (
    lowFreq.isDefined && (lowFreq.get < 6.309573444801933 || lowFreq.get > highFreq.getOrElse(samplingRate/2.0))
  ) {
    throw new IllegalArgumentException(
      s"Incorrect low frequency (${lowFreq.get}) for TOL "
      + " (smaller than 6.309573444801933 or bigger than ${highFreq.getOrElse(samplingRate/2.0)})"
    )
  }

  if (
    highFreq.isDefined
    && (highFreq.get > samplingRate/2.0 || highFreq.get < lowFreq.getOrElse(6.309573444801933))
  ) {
    throw new IllegalArgumentException(
      s"Incorrect high frequency (${highFreq.get}) for TOL "
      + "(higher than ${sampingRate/2.0} or smaller than ${lowFreq.getOrElse(6.309573444801933)})"
    )
  }

  private val expectedWelchSize = if (nfft % 2 == 0) nfft/2 + 1 else (nfft + 1)/2

  /**
   * Generate third octave band boundaries for each center frequency of third octave
   */
  val thirdOctaveBandBounds: Array[(Double, Double)] = {
    // we're using acoustic constants
    // scalastyle:off magic.number

    val tocScalingFactor = math.pow(10, 0.05)



    // See https://en.wikipedia.org/wiki/Octave_band#Base_10_calculation
    // we start at the 8th to-center/ 6.31 Hz,
    // the last band is the last complete before samplingRate/2.0
    val maxTOindex = (10.0*math.log10(samplingRate/2.0)-1.0).toInt
    (8 to maxTOindex)
      // convert third octaves indicies to the frequency of the center of their band
      .map(toIndex => math.pow(10, (0.1 * toIndex)))
      // convert center frequency to a tuple of (lowerBoundFrequency, upperBoundFrequency)
      .map(toCenter => (toCenter / tocScalingFactor, toCenter * tocScalingFactor))
      // keep only the band that are within the study range
      .filter{tob =>
        // partial bands are kept
        (tob._2 >= lowFreq.getOrElse(6.309573444801933)
        && tob._1 <= highFreq.getOrElse(samplingRate / 2.0))
      }
      .toArray
    // scalastyle:on magic.number
  }

  /**
   * Function used to associate a frequency of a spectrum to an index of a discrete
   * nfft-sized spectrum
   *
   * @param freq The frequency to be matched with an index
   * @return The index that corresponds to the given frequency
   */
  def frequencyToIndex(freq: Double): Int = (freq * nfft / samplingRate).toInt

  // Compute the indices associated with each TOB boundary
  private val boundIndicies: Array[(Int, Int)] = thirdOctaveBandBounds.map(
    bound => (frequencyToIndex(bound._1), frequencyToIndex(bound._2))
  )

  /**
   * Function that computes the Third Octave Levels over a PSD
   *
   * Default environmentals parameters ensures that there is no correction on
   * on the third-octave levels.
   *
   * @param welch The one-sided Power Spectral Density as an Array[Double] of length expectedPSDSize
   * TOL can be computed over a periodogram, although, functionnaly, it makes more sense
   * to compute it over a welch estimate of PSD
   * @param vADC The voltage of Analog Digital Converter used in the microphone (given in volts,
   * or in other words ADC peak voltage (e.g 1.414 V)
   * @param microSensitivity Microphone sensitivity (without gain, given in dB)
   * (-170 dB re 1 V/Pa to -140 dB re 1 V/Pa is a range often used)
   * @param gain Gain used for the hydrophones (range from 20 dB to 25 dB, given in dB)
   * @return The Third Octave Levels over the PSD as a Array[Double]
   */
  def compute
  (
    welch: Array[Double],
    vADC: Double = 1.0,
    microSensitivity: Double = 0.0,
    gain: Double = 0.0
  ): Array[Double] = {

    if (welch.length != expectedWelchSize) {
      throw new IllegalArgumentException(
        s"Incorrect PSD size (${welch.length}) for TOL ($expectedWelchSize)"
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
        tol += welch(j)
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
