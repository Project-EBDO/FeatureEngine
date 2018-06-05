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

package org.ode.engine.signal_processing

import scala.math.{cos,Pi,pow,abs,sqrt}

/**
  * Wrapper class over edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D that 
  * computes FFT of nfft size over the signal of length nfft.
  * 
  * Author: Alexandre Degurse
  *
  */


object Energy {

  def computeFromSignal(signal: Array[Double]): Double = {
    signal.foldLeft(0.0)((acc, v) => acc + pow(v,2))
  }

  def computeFromFFT(fft: Array[Double]): Double = {
    val nonNormalizedEnergy = fft
      .foldLeft(0.0)((acc, v) => acc + pow(v,2))
      
    nonNormalizedEnergy / (fft.length / 2.0)
  }

  def computeFromPSD(psd: Array[Double]): Double = {
    psd.sum
  }
}
