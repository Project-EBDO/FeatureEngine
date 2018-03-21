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
import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;

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


object SignalProcessing {

  /**
  * Function that computes FFT for an Seq
  * The segmentation of the signal ensures that signal.length >= nfft
  * @param signal the data to analyze in an Seq[Double]
  * @param nfft number of points of spectrum
  * @return the FFT of the input Seq
  */
  def fft(signal: Seq[Double], nfft : Int) : Seq[Double] = {
    // Compute FFT for Seq double
    val fftClass = new DoubleFFT_1D(nfft);

    // Create an Seq of length 2*signal.length. Indeed, the length doubles
    // because of the FFT imaginary part
    val fft = signal.toArray ++ Array.fill(2*nfft - signal.length)(0.0);

    // Fill temp with all FFT values
    fftClass.realForwardFull(fft) // Side Effect !

    return fft.toSeq
  }
}
