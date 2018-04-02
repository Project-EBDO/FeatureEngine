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

package org.ode.engine.signal_processing;

/**
  * Class that computes FFT of nfft size over the signal of length nfft.
  * Relies on several part of open source codes rearranged to fit Matlab / Python one-sided PSD:
  * https://github.com/lancelet/scalasignal/blob/master/src/main/scala/signal/PSD.scala
  * https://gist.github.com/awekuit/7496127
  * 
  * Author: Paul Nguyen HD, Alexandre Degurse
  */

class PSD(val nfft: Int, val normalisationFactor: Double) {


    val isEven: Boolean = nfft % 2 == 0
    // compute number of unique samples in the transformed FFT
    val nUnique: Int = if (isEven) nfft / 2 + 1 else (nfft+ 1) / 2

  /**
  * Function that computes the one-sided Power Spectral Density (PSD)
  * like in Matlab and Python (mode 'psd')
  * An IllegalArgumentException is thrown if spectrum.length != 2*nfft
  *
  * @param spectrum 
  * @return the PSD of the given spectrum
  */
  def compute(spectrum: Array[Double]) : Array[Double] = {
    if (spectrum.length != (2*nfft)) {
      throw new IllegalArgumentException(s"Incorrect spectrum length (${spectrum.length}) for PSD (${2*nfft})")
    }


    var i: Int = 0
    val oneSidedPSD: Array[Double] = new Array[Double](nUnique)

    while (i < nUnique) {
      oneSidedPSD(i) = normalisationFactor * (spectrum(2*i)*spectrum(2*i) + spectrum(2*i+1)*spectrum(2*i+1))
      i += 1
    }

    // multiply frequencies by 2, except for DC and Nyquist
    val oneSidedSpectrumValues = if (isEven) {
      // if there were an even number of samples, the Nyquist freq is present
      val midFreq = oneSidedPSD.tail.dropRight(1).map(_ * 2.0)
      Array(oneSidedPSD.head) ++ midFreq ++ Array(oneSidedPSD.last)
    } else {
      // for an odd number of samples, there was no Nyquist freq
      val tailFreq = oneSidedPSD.tail.map(_ * 2.0)
      Array(oneSidedPSD.head) ++ tailFreq
    }

    return oneSidedSpectrumValues
  }
}
