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


case class TOL
(
  nfft: Int,
  samplingRate: Float,
  lowFreq: Option[Double] = None,
  highFreq: Option[Double] = None
) extends FrequencyConvertible with Serializable {

  if (nfft < samplingRate) {
    throw new IllegalArgumentException(
      s"Incorrect window size ($nfft) for TOL ($samplingRate)"
    )
  }

  // we're using acoustic constants
  // scalastyle:off magic.number
  private val tocScalingFactor = math.pow(10, 0.05)

  // The lowest accepted value is the 0th band (center at 1Hz),
  private val lowerLimit = 1.0
  // and the highest is the minimum of samplingRate / 2 and the 60th band.
  private val upperLimit = math.min(
    samplingRate / (2.0 ),
    math.pow(10, 0.1 * 60)
  )

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

  /**
   * Generate third octave band boundaries for each center frequency of third octave
   */
  val thirdOctaveBandBounds: Array[(Double, Double)] = {
    // We compute bands for indices from 0 to 60, and then filter them
    // This way of proceeding makes the code easier to understand in comparison to
    // computing the exact boundaries
    // See https://en.wikipedia.org/wiki/Octave_band#Base_10_calculation
    (0 to 60)
      // convert third octaves indicies to the frequency of the center of their band
      .map(toIndex => math.pow(10, (0.1 * toIndex)))
      // scalastyle:on magic.number
      // convert center frequency to a tuple of (lowerBoundFrequency, upperBoundFrequency)
      .map(toCenter => (toCenter / tocScalingFactor, toCenter * tocScalingFactor))
      // keep only the band within the study range
      .filter{tob =>
        /**
         * lower partial band is kept and the upper one is dropped if it exceeds over upperlimit
         * (upperLimit is usually samplingRate/2, in this case,
         * the band overflows the power spectrum limits)
         * and is kept if the upper bound is inferior to upperLimit
         * (ie tob._1 < highFreq < tob ._2 < upperLimit)
         */
        (tob._2 >= lowFreq.getOrElse(lowerLimit)
        && tob._1 <= highFreq.getOrElse(upperLimit)
        && tob._2 < upperLimit)
      }
      .toArray
  }

  // Compute the indices associated with each TOB boundary
  private val boundIndicies: Array[(Int, Int)] = thirdOctaveBandBounds.map(
    bound => (super.frequencyToIndex(bound._1), super.frequencyToIndex(bound._2))
  )

  /**
   * Function converting a frequency to a index in the TOLs
   *
   * @param freq Frequency to be converted
   * @return Index in spectrum that corresponds to the given frequency
   */
  override def frequencyToIndex(freq: Double): Int = {
    if (freq < lowFreq.getOrElse(lowerLimit) || freq > highFreq.getOrElse(upperLimit)) {
      throw new IllegalArgumentException(
        s"Incorrect frequency ($freq) for conversion "
        + s"(${lowFreq.getOrElse(lowerLimit)},${highFreq.getOrElse(upperLimit)})"
      )
    }

    thirdOctaveBandBounds.indexWhere(bound => freq >= bound._1 && freq <= bound._2, 0)
  }

  /**
   * Function converting a index in the TOLs to a frequency
   *
   * @param idx Index to be converted
   * @return Frequency that corresponds to the given index
   */
  override def indexToFrequency(idx: Int): Double = {
    if (idx < 0 || idx >= thirdOctaveBandBounds.length) {
      throw new IllegalArgumentException(
        s"Incorrect index ($idx) for conversion (${thirdOctaveBandBounds.length})"
      )
    }

    thirdOctaveBandBounds(idx)._1
  }

  /**
   * Function computing the frequency vector given a nfft and a samplingRate for TOL
   *
   * @return The frequency vector that corresponds to the current nfft and samplingRate
   */
  override lazy val frequencyVector: Array[Double] = {
    val frequencyVector = new Array[Double](thirdOctaveBandBounds.length + 1)

    (0 until thirdOctaveBandBounds.length).foreach(i =>
      frequencyVector(i) = indexToFrequency(i)
    )

    frequencyVector(thirdOctaveBandBounds.length) = thirdOctaveBandBounds.last._2

    frequencyVector
  }

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
   * @return The Third Octave Levels over the PSD as a Array[Double]
   */
  def compute(spectrum: Array[Double]): Array[Double] = {

    if (spectrum.length != spectrumSize) {
      throw new IllegalArgumentException(
        s"Incorrect PSD size (${spectrum.length}) for TOL ($spectrumSize)"
      )
    }

    val tols = new Array[Double](boundIndicies.length)

    // scalastyle:off while var.local
    var i = 0
    var j = 0
    var tol: Double = 0.0

    // i moves over the TO indices
    while (i < boundIndicies.length) {
      j = boundIndicies(i)._1
      // j moves between a TO low to high band
      while (j < boundIndicies(i)._2) {
        tol += spectrum(j)
        j += 1
      }
      tols(i) = 10.0 * math.log10(tol)
      tol = 0.0
      i += 1
    }
    // scalastyle:on while var.local
    tols
  }
}
