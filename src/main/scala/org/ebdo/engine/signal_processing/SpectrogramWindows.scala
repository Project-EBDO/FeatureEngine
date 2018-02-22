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

import scala.math.{cos,Pi}

/**
  * Spectrogram Windows Functions for signal processing
  * Author: Paul NGuyenhongduc
  *
  * Relies on several part of open source codes by JorenSix :
  * https://github.com/JorenSix/TarsosDSP/tree/master/src/core/be/tarsos/dsp/util/fft
  */

object SpectrogramWindows{
  /**
    * Function that computes a coefficient for Hamming window
    *
    * @param i coefficient to compute
    * @param N length of the window (to match with Nfft)
    * @return one Hamming coefficient
    */
  def hamming(i: Int, N: Int): Double = {
    // Generate the i-th coefficient of a N-point Hamming window
    return 0.54 - 0.46 * cos(2 * Pi * i / (N - 1))
  }

  /**
    * Function that window a signal with a Hamming window
    *
    * @param signal signal to window
    * @return Hamming windowed signal
    */
  def applyHamming(signal: Seq[Double]): Seq[Double] = {
    return signal
      .zipWithIndex
      .map(x => x._1 * hamming(x._2, signal.length))
      .toSeq
  }
}
