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

package org.oceandataexplorer.engine.signalprocessing.windowfunctions

import org.oceandataexplorer.engine.signalprocessing.windowfunctions.WindowFunctionTypes.{Periodic, Symmetric}
import org.oceandataexplorer.utils.test.OdeCustomMatchers
import org.scalatest.{FlatSpec, Matchers}


/**
 * Tests for Hann Window Function
 *
 * @author Alexandre Degurse
 */
class TestHannWindowFunction extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 4e-14

  // make sure to generate results with enough digits :
  //    - "np.set_printoptions(precision=16)" for numpy
  //    - "format long" for Matlab

  "HannWindowFunction" should "rmse-match scipy Periodic hann window" in {
    // scipy.signal.get_window("hann", 32)
    // or
    // scipy.signal.hann(32, False)
    val expectedWindow = Array(
      0.0               , 0.0096073597983848, 0.0380602337443566, 0.0842651938487273,
      0.1464466094067263, 0.222214883490199 , 0.3086582838174551, 0.4024548389919359,
      0.5               , 0.5975451610080642, 0.6913417161825449, 0.7777851165098011,
      0.8535533905932737, 0.9157348061512727, 0.9619397662556434, 0.9903926402016152,
      1.0               , 0.9903926402016152, 0.9619397662556434, 0.9157348061512727,
      0.8535533905932737, 0.7777851165098011, 0.6913417161825451, 0.5975451610080642,
      0.5               , 0.4024548389919358, 0.308658283817455 , 0.222214883490199 ,
      0.1464466094067263, 0.0842651938487273, 0.0380602337443567, 0.0096073597983848
    )

    val hannClass = HannWindowFunction(32, Periodic)
    val window = hannClass.windowCoefficients

    window should rmseMatch(expectedWindow)
  }

  it should "rmse-match a transformed signal as in scipy with a Periodic hann window" in {
    // scipy.signal.get_window("hann", 32) * numpy.arange(1,33)
    val expectedWindowedSignal = Array(
      0.0               ,  0.0192147195967696,  0.1141807012330699,
      0.3370607753949093,  0.7322330470336313,  1.3332893009411941,
      2.160607986722186 ,  3.2196387119354872,  4.5               ,
      5.975451610080642 ,  7.604758878007994 ,  9.333421398117613 ,
      11.096194077712559 , 12.820287286117818 , 14.42909649383465  ,
      15.846282243225843 , 17.0               , 17.827067523629076 ,
      18.276855558857225 , 18.314696123025453 , 17.92462120245875  ,
      17.111272563215625 , 15.900859472198539 , 14.341083864193541 ,
      12.5               , 10.463825813790331 ,  8.333773663071284 ,
      6.222016737725573 ,  4.246951672795062 ,  2.52795581546182  ,
      1.179867246075059 ,  0.3074355135483131
    )

    val hannClass = HannWindowFunction(32, Periodic)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hannClass.applyToSignal(signal)

    windowedSignal should rmseMatch(expectedWindowedSignal)
  }

  it should "rmse-match Symmetric hann window" in {
    // numpy.hanning(32)
    // or
    // scipy.signal.hann(32, True)
    val expectedWindow = Array(
      0.0               , 0.0102350293737528, 0.0405210941898847, 0.0896182793963619,
      0.1555165404621567, 0.2355179948365188, 0.3263473735775899, 0.4242861112477117,
      0.5253245844193564, 0.6253262661293602, 0.7201970757788172, 0.8060529912738312,
      0.8793790613463954, 0.937173308072291 , 0.9770696282000244, 0.9974346616959475,
      0.9974346616959475, 0.9770696282000244, 0.937173308072291 , 0.8793790613463955,
      0.8060529912738313, 0.7201970757788171, 0.6253262661293606, 0.5253245844193568,
      0.4242861112477117, 0.3263473735775899, 0.2355179948365188, 0.1555165404621567,
      0.0896182793963619, 0.0405210941898847, 0.0102350293737529, 0.0
    )

    val hannClass = HannWindowFunction(32, Symmetric)
    val window = hannClass.windowCoefficients

    window should rmseMatch(expectedWindow)
  }

  it should "rmse-match a transformed signal as in scipy with a Symmetric hann window" in {
    // numpy.hanning(32) * numpy.arange(1,33) or scipy.signal.hann(32) * numpy.arange(1,33)
    val expectedWindowedSignal = Array(
      0.0               ,  0.0204700587475055,  0.1215632825696541,
      0.3584731175854474,  0.7775827023107834,  1.4131079690191126,
      2.2844316150431294,  3.3942888899816936,  4.727921259774208 ,
      6.253262661293602 ,  7.92216783356699  ,  9.672635895285978 ,
      11.43192779750314  , 13.120426313012075 , 14.656044423000367 ,
      15.95895458713516  , 16.95638924883111  , 17.587253307600438 ,
      17.80629285337353  , 17.58758122692791  , 16.927112816750455 ,
      15.844335667133976 , 14.382504120975284 , 12.60779002606455  ,
      10.60715278119279  ,  8.485031713017335 ,  6.358985860586007 ,
      4.354463132940387 ,  2.598930102494494 ,  1.215632825696541 ,
      0.3172859105863355,  0.0
    )

    val hannClass = HannWindowFunction(32, Symmetric)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hannClass.applyToSignal(signal)

    windowedSignal should rmseMatch(expectedWindowedSignal)
  }

  it should "compute the right density normalization factor for a Symmetric hann window" in {
    val hannClass = HannWindowFunction(32, Symmetric)
    val normalizationFactor = hannClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hann(32, True)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 3.795918367346939

    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right density normalization factor for a Periodic hann window" in {
    val hannClass = HannWindowFunction(32, Periodic)
    val normalizationFactor = hannClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hann(32, False)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 3.9183673469387754

    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right spectrum normalization factor for a Periodic hann window" in {
    val hannClass = HannWindowFunction(32, Periodic)
    val normalizationFactor = hannClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hann(32, False)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 83.59183673469387

    // spectrum normalization factor are large values => large absolute error
    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right spectrum normalization factor for a Symmetric hann window" in {
    val hannClass = HannWindowFunction(32, Symmetric)
    val normalizationFactor = hannClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hann(32, True)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 78.44897959183675

    // spectrum normalization factor are large values => large absolute error
    normalizationFactor should rmseMatch(expectedNormFactor)
  }


  it should "raise IllegalArgumentException when applying to a signal of the wrong size" in {
    val hannClass = HannWindowFunction(10, Symmetric)
    val signal = new Array[Double](100)

    the[IllegalArgumentException] thrownBy {
      hannClass.applyToSignal(signal)
    } should have message "Incorrect signal length (100) for SpectrogramWindow (10)"
  }
}
