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
 * Class computing energy and Sound Pressure Level from signal information.
 * Can be used over raw signal, FFT one-sided, or PSD (either periodogram
 * or Welch's, one-sided).
 *
 * @author Alexandre Degurse
 *
 * @param nfft The size of the fft-computation window
 */
case class Energy(nfft: Int) extends Serializable {

  private val nfftEven: Boolean = nfft % 2 == 0
  private val expectedFFTSize = nfft + (if (nfftEven) 2 else 1)
  private val expectedPSDSize = expectedFFTSize / 2


  /**
   * Function that provide computation for the energy of a raw signal
   *
   * @param signal The raw signal to process as an Array[Double]
   * @return The energy of the input raw signal as a Double
   */
  def computeRawFromRawSignal(signal: Array[Double]): Double = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(
        s"Incorrect signal size (${signal.length}) for Energy ($nfft)"
      )
    }
    // Using while with local variables on purpose -- See performance test
    // scalastyle:off while var.local
    var i = 0
    var energy = 0.0
    while (i < signal.length) {
      energy += signal(i) * signal(i)
      i += 1
    }
    energy
    // scalastyle:on while var.local
  }


  /**
   * Function that provide computation for the energy of a FFT
   *
   * @param fft The one-sided FFT to process as an Array[Double]
   * @return The energy of the input FFT as a Double
   */
  def computeRawFromFFT(fft: Array[Double]): Double = {

    if (fft.length != expectedFFTSize) {
      throw new IllegalArgumentException(
        s"Incorrect fft size (${fft.length}) for Energy ($expectedFFTSize)"
      )
    }

    // Using while with local variables on purpose -- See performance test
    // scalastyle:off while var.local

    var energy = 0.0
    // start at 1 to not duplicate DC
    var i = 1
    var i2 = 2 * i
    var i2p1 = i2 + 1

    // add the energy of all the points between DC and Nyquist' freq
    // An interesting point to notice is that aggregating values up to nfft / 2 and then adding
    // values for fft.length - 2 and fft.length -2 works for both nfft pair or odd
    // since expectedFFTSize is nfft / 2 + 2 if nfft is even and nfft + 1 if nfft is odd
    while (i < nfft/2) {
      // duplicate the energy due to the symetry
      energy += 2.0 * fft(i2) * fft(i2)
      energy += 2.0 * fft(i2p1) * fft(i2p1)
      i += 1
      i2 = i * 2
      i2p1 = i2 + 1
    }


    // add DC's energy
    energy += fft(0) * fft(0)
    // add the energy around Nyquist' freq
    energy += fft(i2) * fft(i2) * (if (nfftEven) 1.0 else 2.0)
    energy += fft(i2p1) * fft(i2p1) * (if (nfftEven) 1.0 else 2.0)
    // scalastyle:on while var.local

    energy / nfft.toDouble
  }

  /**
   * Function that provide computation for the energy of a PSD
   * (either periodogram or Welch's)
   *
   * @param psd The PSD to process as an Array[Double] (either
   *            periodogram or Welch's)
   * @return The energy of the input PSD as a Double
   */
  def computeRawFromPSD(psd: Array[Double]): Double = {
    if (psd.length != expectedPSDSize) {
      throw new IllegalArgumentException(
        s"Incorrect PSD size (${psd.length}) for Energy ($expectedPSDSize)"
      )
    }

    // Using while with local variables on purpose -- See performance test
    // scalastyle:off while var.local
    var i = 0
    var energy = 0.0
    while (i < psd.length) {
      energy += psd(i)
      i += 1
    }
    energy
    // scalastyle:on while var.local
  }


  /**
   * Function that provide computation for Sound Pressure Level
   * Wrapper over converting raw energy to db
   *
   * @param signal The raw signal to process as an Array[Double]
   * @return The Sound Pressure Level of the input raw signal as a Double
   */
  def computeSPLFromRawSignal(signal: Array[Double]): Double = {
    Energy.toDB(computeRawFromRawSignal(signal))
  }

  /**
   * Function that provide computation for Sound Pressure Level
   * Wrapper over converting raw energy to db
   *
   * @param fft The FFT to process as an Array[Double]
   * @return The Sound Pressure Level of the input FFT as a Double
   */
  def computeSPLFromFFT(fft: Array[Double]): Double = {
    Energy.toDB(computeRawFromFFT(fft))
  }

  /**
   * Function that provide computation for Sound Pressure Level
   * Wrapper over converting raw energy to db
   *
   * @param psd The PSD to process as an Array[Double] (either
   *            periodogram or Welch's)
   * @return The Sound Pressure Level of the input PSD as a Double
   */
  def computeSPLFromPSD(psd: Array[Double]): Double = {
    Energy.toDB(computeRawFromPSD(psd))
  }
}

/**
 * Companion object to the Energy class
 */
object Energy {

  /**
   * Function converting raw energy to dB scale
   *
   * @param value The value to convert to log scala as a Double
   * @return The value converted to log scale as a Double
   */
  def toDB(value: Double): Double = 10.0 * math.log10(value)
}
