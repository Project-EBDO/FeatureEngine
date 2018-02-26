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

package org.ebdo.engine.signal_processing

import org.ebdo.utils.test.ErrorMetrics.rmse
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for Spectrogram Windows Functions
  * Author: Alexandre Degurse
  */

class TestHammingWindow extends FlatSpec with Matchers {
  // windowed signal fails for Matlab and numpy/scipy with maxRMSE lower than 3e-15
  val maxRMSE = 4e-15

  // make sure to generate results with enough digits :
  //    - "np.set_printoptions(precision=16)" for numpy
  //    - "format long" for Matlab

  "SpectrogramWindows" should "generate the same hamming window as numpy/scipy" in {
    // numpy.hamming(32) or scipy.signal.hamming(32)
    val expectedWindow = Array(
      0.08              , 0.0894162270238525, 0.1172794066546939,
      0.1624488170446529, 0.2230752172251842, 0.2966765552495972,
      0.3802395836913828, 0.4703432223478948, 0.5632986176658079,
      0.6553001648390115, 0.7425813097165119, 0.821568751971925 ,
      0.889028736438684 , 0.9421994434265079, 0.9789040579440225,
      0.9976398887602718, 0.9976398887602718, 0.9789040579440225,
      0.9421994434265079, 0.889028736438684 , 0.8215687519719248,
      0.7425813097165117, 0.6553001648390114, 0.5632986176658078,
      0.4703432223478947, 0.3802395836913827, 0.2966765552495972,
      0.2230752172251842, 0.1624488170446529, 0.1172794066546939,
      0.0894162270238525, 0.08 )


    val hw = new HammingWindow(32)
    val window = hw.windowCoefficients

    rmse(expectedWindow,window) should be < (maxRMSE)
  }

  "SpectrogramWindows" should "generate the same windowed signal as numpy/scipy" in {
    // numpy.hamming(32) * np.arange(1,33) or scipy.signal.hamming(32) * np.arange(1,33)
    val expectedWindowedSignal = Array(
      0.08              ,  0.1788324540477051,  0.3518382199640818,
      0.6497952681786117,  1.115376086125921 ,  1.7800593314975834,
      2.6616770858396794,  3.7627457787831586,  5.069687558992271 ,
      6.553001648390115 ,  8.168394406881632 ,  9.858825023663101 ,
      11.557373573702892 , 13.19079220797111  , 14.683560869160337,
      15.962238220164348 , 16.95987810892462  , 17.620273042992405,
      17.90178942510365  , 17.78057472877368  , 17.25294379141042 ,
      16.336788813763256 , 15.071903791297263 , 13.519166823979386,
      11.758580558697368 ,  9.886229175975949 ,  8.010266991739126,
      6.2461060823051575,  4.711015694294934 ,  3.5183821996408184,
      2.771903037739429 ,  2.5600000000000005)

    val hw = new HammingWindow(32)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hw.applyToSignal(signal)

    rmse(expectedWindowedSignal,windowedSignal) should be < (maxRMSE)
  }
}
