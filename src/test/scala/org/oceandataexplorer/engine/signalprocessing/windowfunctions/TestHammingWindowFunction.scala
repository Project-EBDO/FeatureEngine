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

import WindowFunctionTypes.{Symmetric, Periodic}
import org.oceandataexplorer.utils.test.ErrorMetrics
import org.scalatest.{FlatSpec, Matchers}


/**
 * Tests for Spectrogram Windows Functions
 *
 * @author Alexandre Degurse
 */
class TestHammingWindowFunction extends FlatSpec with Matchers {
  // windowed signal fails for Matlab and numpy/scipy with maxRMSE lower than 3e-15
  private val maxRMSE = 4e-15

  // make sure to generate results with enough digits :
  //    - "np.set_printoptions(precision=16)" for numpy
  //    - "format long" for Matlab

  "HammingWindowFunction" should "rmse-match scipy Periodic hamming window" in {
    // scipy.signal.get_window("hamming", 32)
    // or
    // scipy.signal.hamming(32, False)
    val expectedWindow = Array(
      0.08              , 0.088838771014514 , 0.1150154150448081,
      0.1575239783408292, 0.2147308806541882, 0.2844376928109831,
      0.3639656211120588, 0.4502584518725811, 0.54              ,
      0.6297415481274191, 0.7160343788879413, 0.795562307189017 ,
      0.865269119345812 , 0.922476021659171 , 0.9649845849551919,
      0.9911612289854861, 1.0               , 0.9911612289854861,
      0.9649845849551919, 0.922476021659171 , 0.865269119345812 ,
      0.795562307189017 , 0.7160343788879415, 0.6297415481274191,
      0.54              , 0.450258451872581 , 0.3639656211120585,
      0.2844376928109831, 0.2147308806541882, 0.1575239783408292,
      0.1150154150448082, 0.088838771014514 )


    val hammingClass = new HammingWindowFunction(32, Periodic)
    val window = hammingClass.windowCoefficients

    ErrorMetrics.rmse(expectedWindow,window) should be < maxRMSE
  }

  it should "rmse-match a transformed signal as in scipy with a Periodic hamming window" in {
    // signal.get_window("hamming", 32) * np.arange(1,33)
    val expectedWindowedSignal = Array(
      0.08               ,  0.1776775420290281,  0.3450462451344243,
      0.6300959133633166 ,  1.073654403270941 ,  1.706626156865899 ,
      2.547759347784411  ,  3.6020676149806485,  4.86              ,
      6.297415481274191  ,  7.8763781677673546,  9.546747686268205 ,
      11.248498551495555 , 12.914664303228394 , 14.47476877432788  ,
      15.858579663767777 , 17.0               , 17.84090212173875  ,
      18.334707114148646 , 18.44952043318342  , 18.17065150626205  ,
      17.502370758158374 , 16.468790714422656 , 15.113797155058059 ,
      13.5               , 11.706719748687105 ,  9.827071770025581 ,
      7.964255398707528  ,  6.227195538971459 ,  4.725719350224875 ,
      3.5654778663890547 ,  2.8428406724644493
    )

    val hammingClass = new HammingWindowFunction(32, Periodic)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hammingClass.applyToSignal(signal)

    ErrorMetrics.rmse(expectedWindowedSignal,windowedSignal) should be < maxRMSE
  }

  it should "rmse-match numpy hamming window (Symmetric)" in {
    // numpy.hamming(32)
    // or
    // scipy.signal.hamming(32, True)
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


    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val window = hammingClass.windowCoefficients

    ErrorMetrics.rmse(expectedWindow,window) should be < maxRMSE
  }

  it should "compute the right density normalization factor for a Symmetric hamming window" in {
    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val normalizationFactor = hammingClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hamming(32, True)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 4.024751020408163

    ErrorMetrics.rmse(expectedNormFactor,normalizationFactor) should be < maxRMSE
  }

  it should "compute the right density normalization factor for a Periodic hamming window" in {
    val hammingClass = new HammingWindowFunction(32, Periodic)
    val normalizationFactor = hammingClass.densityNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hamming(32, False)
     * numpy.sum(h * h) / (1.75 ** 2)
     */
    val expectedNormFactor = 4.152424489795918

    ErrorMetrics.rmse(expectedNormFactor,normalizationFactor) should be < maxRMSE
  }

  it should "compute the right spectrum normalization factor for a Periodic hamming window" in {
    val hammingClass = new HammingWindowFunction(32, Periodic)
    val normalizationFactor = hammingClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hamming(32, True)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 97.50151836734696

    // spectrum normalization factor are large values => large absolute error
    ErrorMetrics.rmse(expectedNormFactor,normalizationFactor) should be < 2.1E-14
  }

  it should "compute the right spectrum normalization factor for a Symmetric hamming window" in {
    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val normalizationFactor = hammingClass.spectrumNormalizationFactor(1.75)

    /**
     * h = scipy.signal.hamming(32, False)
     * (numpy.sum(h) / 1.75) ** 2
     */
    val expectedNormFactor = 92.37955918367348

    // spectrum normalization factor are large values => large absolute error
    ErrorMetrics.rmse(expectedNormFactor,normalizationFactor) should be < 2.1E-14
  }

  it should "rmse-match a transformed signal as in numpy" in {
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

    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hammingClass.applyToSignal(signal)

    ErrorMetrics.rmse(expectedWindowedSignal,windowedSignal) should be < maxRMSE
  }

  it should "rmse-match a Symmetric matlab hamming window" in {
    // hamming(32)
    val expectedWindow = Array(
      0.080000000000000016,0.089416227023852546,0.117279406654693941,
      0.162448817044652916,0.223075217225184197,0.296676555249597351,
      0.380239583691382665,0.470343222347894718,0.563298617665807799,
      0.655300164839011545,0.742581309716511928,0.821568751971925026,
      0.889028736438683853,0.942199443426507743,0.978904057944022465,
      0.997639888760271765,0.997639888760271765,0.978904057944022465,
      0.942199443426507743,0.889028736438683853,0.821568751971925026,
      0.742581309716511928,0.655300164839011545,0.563298617665807799,
      0.470343222347894718,0.380239583691382665,0.296676555249597351,
      0.223075217225184197,0.162448817044652916,0.117279406654693941,
      0.089416227023852546,0.080000000000000016
    )

    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val window = hammingClass.windowCoefficients

    ErrorMetrics.rmse(expectedWindow,window) should be < maxRMSE
  }

  it should "rmse-match a transformed signal as in matlab with a Symmetric hamming window" in {
    // hamming(32) .* [1:32](:)
    val expectedWindowedSignal = Array(
      0.080000000000000016,0.178832454047705092,0.351838219964081822,
      0.649795268178611662,1.115376086125920985,1.780059331497584107,
      2.661677085839678547,3.762745778783157746,5.069687558992270304,
      6.553001648390115008,8.168394406881631653,9.858825023663101206,
      11.557373573702889757,13.190792207971108851,14.683560869160336537,
      15.962238220164348235,16.959878108924620221,17.620273042992405266,
      17.901789425103647346,17.780574728773675730,17.252943791410427110,
      16.336788813763263306,15.071903791297264874,13.519166823979386294,
      11.758580558697367735,9.886229175975948635,8.010266991739127818,
      6.246106082305157514,4.711015694294934164,3.518382199640818442,
      2.771903037739428921,2.560000000000000497
    )

    val hammingClass = new HammingWindowFunction(32, Symmetric)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hammingClass.applyToSignal(signal)

    ErrorMetrics.rmse(expectedWindowedSignal,windowedSignal) should be < maxRMSE
  }

  it should "rmse-match a Periodic matlab hamming window" in {
    // hamming(32)
    val expectedWindow = Array(
      0.080000000000000, 0.088838771014514, 0.115015415044808, 0.157523978340829,
      0.214730880654188, 0.284437692810983, 0.363965621112059, 0.450258451872581,
      0.540000000000000, 0.629741548127419, 0.716034378887941, 0.795562307189017,
      0.865269119345812, 0.922476021659171, 0.964984584955192, 0.991161228985486,
      1.000000000000000, 0.991161228985486, 0.964984584955192, 0.922476021659171,
      0.865269119345812, 0.795562307189017, 0.716034378887941, 0.629741548127419,
      0.540000000000000, 0.450258451872581, 0.363965621112059, 0.284437692810983,
      0.214730880654188, 0.157523978340829, 0.115015415044808, 0.088838771014514
    )

    val hammingClass = new HammingWindowFunction(32, Periodic)
    val window = hammingClass.windowCoefficients

    ErrorMetrics.rmse(expectedWindow,window) should be < maxRMSE
  }

  it should "rmse-match a transformed signal as in matlab with a Periodic hamming window" in {
    // hamming(32) .* [1:32](:)
    val expectedWindowedSignal = Array(
      0.080000000000000, 0.177677542029028, 0.345046245134424, 0.630095913363317,
      1.073654403270941, 1.706626156865898, 2.547759347784411, 3.602067614980648,
      4.860000000000000, 6.297415481274190, 7.876378167767355, 9.546747686268205,
      11.248498551495555, 12.914664303228394, 14.474768774327879, 15.858579663767777,
      17.000000000000000, 17.840902121738750, 18.334707114148646, 18.449520433183419,
      18.170651506262050, 17.502370758158374, 16.468790714422649, 15.113797155058055,
      13.500000000000000, 11.706719748687107, 9.827071770025585, 7.964255398707524,
      6.227195538971457, 4.725719350224876, 3.565477866389052, 2.842840672464449
    )

    val hammingClass = new HammingWindowFunction(32, Periodic)
    val signal = (1.0 to 32.0 by 1.0).toArray
    val windowedSignal = hammingClass.applyToSignal(signal)

    ErrorMetrics.rmse(expectedWindowedSignal,windowedSignal) should be < maxRMSE
  }

  it should "raise IllegalArgumentException when applying to a signal of the wrong size" in {
    val hammingClass = new HammingWindowFunction(10, Symmetric)
    val signal = new Array[Double](100)

    the[IllegalArgumentException] thrownBy {
      hammingClass.applyToSignal(signal)
    } should have message "Incorrect signal length (100) for SpectrogramWindow (10)"
  }
}
