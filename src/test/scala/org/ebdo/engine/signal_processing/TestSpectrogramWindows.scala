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

class TestSpectrogramWindows extends FlatSpec with Matchers {
  // windowed signal fails for Matlab and numpy/scipy with maxRMSE lower than 3e-15
  val maxRMSE = 4e-15

  // make sure to generate results with enough digits :
  //    - "np.set_printoptions(precision=16)" for numpy
  //    - "format long" for Matlab

  "SpectrogramWindows" should "generate the same hamming window as numpy/scipy" in {
    // numpy.hamming(32) or scipy.signal.hamming(32)
    val expectedWindow = Seq(
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

    val sw = new SpectrogramWindows(("hamming",32))
    val window = sw.getWindow("hamming",32)

    rmse(expectedWindow,window) should be < (maxRMSE)
  }

  "SpectrogramWindows" should "generate the same windowed signal as numpy/scipy" in {
    // numpy.hamming(32) * np.arange(1,33) or scipy.signal.hamming(32) * np.arange(1,33)
    val expectedWindowedSignal = Seq(
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

    val sw = new SpectrogramWindows(("hamming",32))
    val signal = (1.0 to 32.0 by 1.0).toSeq
    val windowedSignal = sw.applyWindow(signal, "hamming")

    rmse(expectedWindowedSignal,windowedSignal) should be < (maxRMSE)
  }

  "SpectrogramWindows" should "generate the same hamming window as Matlab" in {
    // hamming(32)
    val expectedWindow = Seq(
      0.0800000000000000, 0.0894162270238525, 0.1172794066546939, 0.1624488170446529,
      0.2230752172251842, 0.2966765552495974, 0.3802395836913827, 0.4703432223478947,
      0.5632986176658078, 0.6553001648390115, 0.7425813097165119, 0.8215687519719250,
      0.8890287364386839, 0.9421994434265077, 0.9789040579440225, 0.9976398887602718,
      0.9976398887602718, 0.9789040579440225, 0.9421994434265077, 0.8890287364386839,
      0.8215687519719250, 0.7425813097165119, 0.6553001648390115, 0.5632986176658078,
      0.4703432223478947, 0.3802395836913827, 0.2966765552495974, 0.2230752172251842,
      0.1624488170446529, 0.1172794066546939, 0.0894162270238525, 0.0800000000000000)

    val sw = new SpectrogramWindows(("hamming",32))
    val window = sw.getWindow("hamming",32)

    rmse(expectedWindow,window) should be < (maxRMSE)
  }

  "SpectrogramWindows" should "generate the same windowed signal as Matlab" in {
    // diag(hamming(32) .* [1:32])
    val expectedWindowedSignal = Seq(
      0.0800000000000000, 0.1788324540477051, 0.3518382199640818, 0.6497952681786117,
      1.1153760861259210, 1.7800593314975841, 2.6616770858396785, 3.7627457787831577,
      5.0696875589922703, 6.5530016483901150, 8.1683944068816317, 9.8588250236631012,
      11.5573735737028898, 13.1907922079711089, 14.6835608691603365, 15.9622382201643482,
      16.9598781089246202, 17.6202730429924053, 17.9017894251036473, 17.7805747287736757,
      17.2529437914104271, 16.3367888137632633, 15.0719037912972649, 13.5191668239793863,
      11.7585805586973677, 9.8862291759759486, 8.0102669917391278, 6.2461060823051575,
      4.7110156942949342, 3.5183821996408184, 2.7719030377394289, 2.5600000000000005)

    val sw = new SpectrogramWindows(("hamming",32))
    val signal = (1.0 to 32.0 by 1.0).toSeq
    val windowedSignal = sw.applyWindow(signal, "hamming")

    rmse(expectedWindowedSignal,windowedSignal) should be < (maxRMSE)
  }

  "SpectrogramWindows" should "fail when asked a window type that doesn't exit" in {
    val sw = new SpectrogramWindows(("hamming",32))

    an [IllegalArgumentException] should be thrownBy sw.getWindow("wrongWindow",20)
  }

  "SpectrogramWindows" should "store computed windows" in {
    val windowsPrecomputed = Seq(("hamming",32),("hamming",64),("hamming",128))
    val sw = new SpectrogramWindows(windowsPrecomputed)

    val storedWindows = sw.listOfWindows

    windowsPrecomputed.map(win =>
      storedWindows.contains(win) should be (true)
    )

    val newWin = sw.getWindow("hamming",256)
    val computedWins = windowsPrecomputed ++ Seq(("hamming",256))

    val newStoredWindows = sw.listOfWindows

    computedWins.map(win =>
      newStoredWindows.contains(win) should be (true)
    )
  }
}
