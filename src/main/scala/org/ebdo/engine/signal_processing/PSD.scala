/** Copyright (C) 2017 Project-EBDO
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

package org.ebdo.engine.signal_processing;

import scala.math.{cos,Pi,pow,abs,sqrt}

/**
  * Generic functions for signal processing
  * Author: Paul Nguyen HD, Alexandre Degurse
  *
  * Computes FFT or PSD on a dataset of wav portions
  * Relies on several part of open source codes rearranged to fit Matlab / Python one-sided PSD:
  * https://github.com/lancelet/scalasignal/blob/master/src/main/scala/signal/PSD.scala
  * https://gist.github.com/awekuit/7496127
  *
  */


object PSD {

  /**
  * Function that computes the one-sided Power Spectral Density (PSD)
  * like in Matlab and Python (mode 'psd')
  *
  * @param spectrum 
  * @return the PSD of the given spectrum
  */
  def compute(spectrum: Array[Double], normalisationFactor: Double) : Array[Double] = {
    val isEven: Boolean = spectrum.length % 2 == 0
    // compute number of unique samples in the transformed FFT
    val nUnique: Int = if (isEven) {
        spectrum.length / 2 + 1
      } else {
        (spectrum.length + 1) / 2
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
