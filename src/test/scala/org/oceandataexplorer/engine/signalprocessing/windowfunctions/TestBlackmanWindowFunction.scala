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
 * Tests for Blackman Window Function
 *
 * @author Alexandre Degurse
 */
class TestBlackmanWindowFunction extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 4e-14

  // make sure to generate results with enough digits :
  //    - "np.set_printoptions(precision=16)" for numpy
  //    - "format long" for Matlab

  "BlackmanWindowFunction" should "rmse-match scipy Periodic blackman window" in {
    // scipy.signal.get_window("blackman", 32)
    // or
    // scipy.signal.blackman(32, False)
    val expectedWindow = Array(
      -1.3877787807814457e-17,  3.5177223992876872e-03,  1.4628776239280404e-02,
      3.4879868437934516e-02,  6.6446609406726240e-02,  1.1160020890099179e-01,
      1.7208974132253130e-01,  2.4854447639103294e-01,  3.4000000000000002e-01,
      4.4363479840716119e-01,  5.5477317368762114e-01,  6.6717044192059383e-01,
      7.7355339059327377e-01,  8.6634948074047979e-01,  9.3850830875056723e-01,
      9.8430300280251803e-01,  9.9999999999999989e-01,  9.8430300280251803e-01,
      9.3850830875056723e-01,  8.6634948074047979e-01,  7.7355339059327377e-01,
      6.6717044192059383e-01,  5.5477317368762136e-01,  4.4363479840716136e-01,
      3.4000000000000002e-01,  2.4854447639103283e-01,  1.7208974132253116e-01,
      1.1160020890099179e-01,  6.6446609406726240e-02,  3.4879868437934516e-02,
      1.4628776239280473e-02,  3.5177223992876872e-03
    )

    val blackmanClass = BlackmanWindowFunction(32, Periodic)
    val window = blackmanClass.windowCoefficients

    window should rmseMatch(expectedWindow)
  }

  it should "rmse-match a transformed signal as in scipy with a Periodic blackman window" in {
    // scipy.signal.get_window("blackman", 32) * numpy.arange(1,33)
    val expectedWindowedSignal = Array(
      -1.3877787807814457e-17,  7.0354447985753743e-03,  4.3886328717841212e-02,
      1.3951947375173807e-01,  3.3223304703363121e-01,  6.6960125340595078e-01,
      1.2046281892577191e+00,  1.9883558111282635e+00,  3.0600000000000001e+00,
      4.4363479840716122e+00,  6.1025049105638320e+00,  8.0060453030471255e+00,
      1.0056194077712560e+01,  1.2128892730366717e+01,  1.4077624631258509e+01,
      1.5748848044840289e+01,  1.6999999999999996e+01,  1.7717454050445326e+01,
      1.7831657866260777e+01,  1.7326989614809595e+01,  1.6244621202458749e+01,
      1.4677749722253065e+01,  1.2759782994815291e+01,  1.0647235161771873e+01,
      8.5000000000000000e+00,  6.4621563861668534e+00,  4.6464230157083417e+00,
      3.1248058492277702e+00,  1.9269516727950609e+00,  1.0463960531380354e+00,
      4.5349206341769466e-01,  1.1256711677720599e-01
    )

    val blackmanClass = BlackmanWindowFunction(32, Periodic)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = blackmanClass.applyToSignal(signal)

    windowedSignal should rmseMatch(expectedWindowedSignal)
  }

  it should "rmse-match Symmetric blackman window" in {
    // numpy.blackman(32)
    // or
    // scipy.signal.blackman(32, True)
    val expectedWindow = Array(
      -1.3877787807814457e-17,  3.7516543033711963e-03,  1.5638447715939613e-02,
      3.7402699623947461e-02,  7.1464606955059656e-02,  1.2028646271190804e-01,
      1.8564672376216657e-01,  2.6795497073570773e-01,  3.6573503854800471e-01,
      4.7537853683779363e-01,  5.9122859717500409e-01,  7.0600078869313354e-01,
      8.1149328354676153e-01,  8.9949042889844799e-01,  9.6273070349660639e-01,
      9.9579705699614696e-01,  9.9579705699614718e-01,  9.6273070349660639e-01,
      8.9949042889844810e-01,  8.1149328354676176e-01,  7.0600078869313376e-01,
      5.9122859717500409e-01,  4.7537853683779413e-01,  3.6573503854800515e-01,
      2.6795497073570773e-01,  1.8564672376216657e-01,  1.2028646271190804e-01,
      7.1464606955059684e-02,  3.7402699623947447e-02,  1.5638447715939585e-02,
      3.7516543033712380e-03, -1.3877787807814457e-17
    )


    val blackmanClass = BlackmanWindowFunction(32, Symmetric)
    val window = blackmanClass.windowCoefficients

    window should rmseMatch(expectedWindow)
  }

  it should "rmse-match a transformed signal as in scipy with a Symmetric blackman window" in {
    // numpy.blackman(32) * numpy.arange(1,33) or scipy.signal.blackman(32) * numpy.arange(1,33)
    val expectedWindowedSignal = Array(
      -1.3877787807814457e-17,  7.5033086067423926e-03,  4.6915343147818818e-02,
      1.4961079849578984e-01,  3.5732303477529825e-01,  7.2171877627144787e-01,
      1.2995270663351663e+00,  2.1436397658856623e+00,  3.2916153469320424e+00,
      4.7537853683779376e+00,  6.5035145689250466e+00,  8.4720094643176065e+00,
      1.0549412686107901e+01,  1.2592866004578273e+01,  1.4440960552449097e+01,
      1.5932752911938355e+01,  1.6928549968934501e+01,  1.7329152662938917e+01,
      1.7090318149070512e+01,  1.6229865670935233e+01,  1.4826016562555804e+01,
      1.3007029137850088e+01,  1.0933706347269254e+01,  8.7776409251521095e+00,
      6.6988742683926921e+00,  4.8268148178163299e+00,  3.2477344932215155e+00,
      2.0010089947416705e+00,  1.0846782890944764e+00,  4.6915343147818817e-01,
      1.1630128340450709e-01, -4.4408920985006262e-16
    )

    val blackmanClass = BlackmanWindowFunction(32, Symmetric)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = blackmanClass.applyToSignal(signal)

    windowedSignal should rmseMatch(expectedWindowedSignal)
  }

  it should "compute the right density normalization factor for a Symmetric blackman window" in {
    val blackmanClass = BlackmanWindowFunction(32, Symmetric)
    val normalizationFactor = blackmanClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.blackman(32, True)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 3.0832979591836738

    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right density normalization factor for a Periodic blackman window" in {
    val blackmanClass = BlackmanWindowFunction(32, Periodic)
    val normalizationFactor = blackmanClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.blackman(32, False)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 3.182759183673469

    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right spectrum normalization factor for a Periodic blackman window" in {
    val blackmanClass = BlackmanWindowFunction(32, Periodic)
    val normalizationFactor = blackmanClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.blackman(32, False)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 58.9824

    // spectrum normalization factor are large values => large absolute error
    normalizationFactor should rmseMatch(expectedNormFactor)
  }

  it should "compute the right spectrum normalization factor for a Symmetric blackman window" in {
    val blackmanClass = BlackmanWindowFunction(32, Symmetric)
    val normalizationFactor = blackmanClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.blackman(32, True)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 55.35359999999999

    // spectrum normalization factor are large values => large absolute error
    normalizationFactor should rmseMatch(expectedNormFactor)
  }


  it should "raise IllegalArgumentException when applying to a signal of the wrong size" in {
    val blackmanClass = BlackmanWindowFunction(10, Symmetric)
    val signal = new Array[Double](100)

    the[IllegalArgumentException] thrownBy {
      blackmanClass.applyToSignal(signal)
    } should have message "Incorrect signal length (100) for SpectrogramWindow (10)"
  }
}
