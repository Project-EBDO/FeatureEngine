/** Copyright (C) 2017-2018 Project-ODE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.0 See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.0 If not, see <http://www.gnu.org/licenses/>.
 */

package org.ode.engine.signal_processing

import org.ode.utils.test.ErrorMetrics
import org.scalatest.{FlatSpec, Matchers}

/**
 * FrequencyConverter test implementation
 *
 * @author Alexandre Degurse
 */

case class TestClass(nfft: Int, samplingRate: Float) extends FrequencyConvertible

class TestFrequencyConvertible extends FlatSpec with Matchers {

  val testClass0 = TestClass(100, 1.0f)
  val testClass1 = TestClass(1024, 44800.0f)
  val testClass2 = TestClass(2049, 96000.0f)

  it should "compute the right frequency when given spectrum indicies" in {
    testClass0.indexToFrequency(0) should equal(0.0)
    testClass0.indexToFrequency(10) should equal(0.1)
    testClass0.indexToFrequency(25) should equal(0.25)
    testClass0.indexToFrequency(50) should equal(0.5)

    testClass1.indexToFrequency(0) should equal(0.0)
    testClass1.indexToFrequency(1) should equal(43.75)
    testClass1.indexToFrequency(100) should equal(4375.0)

    testClass2.indexToFrequency(0) should equal(0.0)
    testClass2.indexToFrequency(1) should equal(46.85212298682284)
    testClass2.indexToFrequency(1024) should equal(47976.57393850659)
  }

  it should "compute the right index in the spectrum given frequencies" in {
    testClass0.frequencyToIndex(0.0) should equal(0)
    testClass0.frequencyToIndex(0.1) should equal(10)
    testClass0.frequencyToIndex(0.25) should equal(25)
    testClass0.frequencyToIndex(0.5) should equal(50)

    testClass1.frequencyToIndex(43.75) should equal(1)
    testClass1.frequencyToIndex(4375.0) should equal(100)

    testClass2.frequencyToIndex(46.85212298682284) should equal(1)
    testClass2.frequencyToIndex(47976.57393850659) should equal(1024)
  }

  it should "compute the right frequency ranges" in {
    // freqVect0, p = scipy.signal.periodogram(numpy.arange(100),1.0)
    val freqVect0 = Array(
      0.0 , 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1 ,
      0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2 , 0.21,
      0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.3 , 0.31, 0.32,
      0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.4 , 0.41, 0.42, 0.43,
      0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.5
    )

    // freqVect1, p = scipy.signal.periodogram(numpy.arange(1024),44800.0)
    val freqVect1 = Array(
      0.0 ,    43.75,    87.5 ,   131.25,   175.0 ,   218.75,   262.5 ,   306.25,   350.0 ,
      393.75,   437.5 ,   481.25,   525.0 ,   568.75,   612.5 ,   656.25,   700.0 ,   743.75,
      787.5 ,   831.25,   875.0 ,   918.75,   962.5 ,  1006.25,  1050.0 ,  1093.75,  1137.5 ,
      1181.25,  1225.0 ,  1268.75,  1312.5 ,  1356.25,  1400.0 ,  1443.75,  1487.5 ,  1531.25,
      1575.0 ,  1618.75,  1662.5 ,  1706.25,  1750.0 ,  1793.75,  1837.5 ,  1881.25,  1925.0 ,
      1968.75,  2012.5 ,  2056.25,  2100.0 ,  2143.75,  2187.5 ,  2231.25,  2275.0 ,  2318.75,
      2362.5 ,  2406.25,  2450.0 ,  2493.75,  2537.5 ,  2581.25,  2625.0 ,  2668.75,  2712.5 ,
      2756.25,  2800.0 ,  2843.75,  2887.5 ,  2931.25,  2975.0 ,  3018.75,  3062.5 ,  3106.25,
      3150.0 ,  3193.75,  3237.5 ,  3281.25,  3325.0 ,  3368.75,  3412.5 ,  3456.25,  3500.0 ,
      3543.75,  3587.5 ,  3631.25,  3675.0 ,  3718.75,  3762.5 ,  3806.25,  3850.0 ,  3893.75,
      3937.5 ,  3981.25,  4025.0 ,  4068.75,  4112.5 ,  4156.25,  4200.0 ,  4243.75,  4287.5 ,
      4331.25,  4375.0 ,  4418.75,  4462.5 ,  4506.25,  4550.0 ,  4593.75,  4637.5 ,  4681.25,
      4725.0 ,  4768.75,  4812.5 ,  4856.25,  4900.0 ,  4943.75,  4987.5 ,  5031.25,  5075.0 ,
      5118.75,  5162.5 ,  5206.25,  5250.0 ,  5293.75,  5337.5 ,  5381.25,  5425.0 ,  5468.75,
      5512.5 ,  5556.25,  5600.0 ,  5643.75,  5687.5 ,  5731.25,  5775.0 ,  5818.75,  5862.5 ,
      5906.25,  5950.0 ,  5993.75,  6037.5 ,  6081.25,  6125.0 ,  6168.75,  6212.5 ,  6256.25,
      6300.0 ,  6343.75,  6387.5 ,  6431.25,  6475.0 ,  6518.75,  6562.5 ,  6606.25,  6650.0 ,
      6693.75,  6737.5 ,  6781.25,  6825.0 ,  6868.75,  6912.5 ,  6956.25,  7000.0 ,  7043.75,
      7087.5 ,  7131.25,  7175.0 ,  7218.75,  7262.5 ,  7306.25,  7350.0 ,  7393.75,  7437.5 ,
      7481.25,  7525.0 ,  7568.75,  7612.5 ,  7656.25,  7700.0 ,  7743.75,  7787.5 ,  7831.25,
      7875.0 ,  7918.75,  7962.5 ,  8006.25,  8050.0 ,  8093.75,  8137.5 ,  8181.25,  8225.0 ,
      8268.75,  8312.5 ,  8356.25,  8400.0 ,  8443.75,  8487.5 ,  8531.25,  8575.0 ,  8618.75,
      8662.5 ,  8706.25,  8750.0 ,  8793.75,  8837.5 ,  8881.25,  8925.0 ,  8968.75,  9012.5 ,
      9056.25,  9100.0 ,  9143.75,  9187.5 ,  9231.25,  9275.0 ,  9318.75,  9362.5 ,  9406.25,
      9450.0 ,  9493.75,  9537.5 ,  9581.25,  9625.0 ,  9668.75,  9712.5 ,  9756.25,  9800.0 ,
      9843.75,  9887.5 ,  9931.25,  9975.0 , 10018.75, 10062.5 , 10106.25, 10150.0 , 10193.75,
      10237.5 , 10281.25, 10325.0 , 10368.75, 10412.5 , 10456.25, 10500.0 , 10543.75, 10587.5 ,
      10631.25, 10675.0 , 10718.75, 10762.5 , 10806.25, 10850.0 , 10893.75, 10937.5 , 10981.25,
      11025.0 , 11068.75, 11112.5 , 11156.25, 11200.0 , 11243.75, 11287.5 , 11331.25, 11375.0 ,
      11418.75, 11462.5 , 11506.25, 11550.0 , 11593.75, 11637.5 , 11681.25, 11725.0 , 11768.75,
      11812.5 , 11856.25, 11900.0 , 11943.75, 11987.5 , 12031.25, 12075.0 , 12118.75, 12162.5 ,
      12206.25, 12250.0 , 12293.75, 12337.5 , 12381.25, 12425.0 , 12468.75, 12512.5 , 12556.25,
      12600.0 , 12643.75, 12687.5 , 12731.25, 12775.0 , 12818.75, 12862.5 , 12906.25, 12950.0 ,
      12993.75, 13037.5 , 13081.25, 13125.0 , 13168.75, 13212.5 , 13256.25, 13300.0 , 13343.75,
      13387.5 , 13431.25, 13475.0 , 13518.75, 13562.5 , 13606.25, 13650.0 , 13693.75, 13737.5 ,
      13781.25, 13825.0 , 13868.75, 13912.5 , 13956.25, 14000.0 , 14043.75, 14087.5 , 14131.25,
      14175.0 , 14218.75, 14262.5 , 14306.25, 14350.0 , 14393.75, 14437.5 , 14481.25, 14525.0 ,
      14568.75, 14612.5 , 14656.25, 14700.0 , 14743.75, 14787.5 , 14831.25, 14875.0 , 14918.75,
      14962.5 , 15006.25, 15050.0 , 15093.75, 15137.5 , 15181.25, 15225.0 , 15268.75, 15312.5 ,
      15356.25, 15400.0 , 15443.75, 15487.5 , 15531.25, 15575.0 , 15618.75, 15662.5 , 15706.25,
      15750.0 , 15793.75, 15837.5 , 15881.25, 15925.0 , 15968.75, 16012.5 , 16056.25, 16100.0 ,
      16143.75, 16187.5 , 16231.25, 16275.0 , 16318.75, 16362.5 , 16406.25, 16450.0 , 16493.75,
      16537.5 , 16581.25, 16625.0 , 16668.75, 16712.5 , 16756.25, 16800.0 , 16843.75, 16887.5 ,
      16931.25, 16975.0 , 17018.75, 17062.5 , 17106.25, 17150.0 , 17193.75, 17237.5 , 17281.25,
      17325.0 , 17368.75, 17412.5 , 17456.25, 17500.0 , 17543.75, 17587.5 , 17631.25, 17675.0 ,
      17718.75, 17762.5 , 17806.25, 17850.0 , 17893.75, 17937.5 , 17981.25, 18025.0 , 18068.75,
      18112.5 , 18156.25, 18200.0 , 18243.75, 18287.5 , 18331.25, 18375.0 , 18418.75, 18462.5 ,
      18506.25, 18550.0 , 18593.75, 18637.5 , 18681.25, 18725.0 , 18768.75, 18812.5 , 18856.25,
      18900.0 , 18943.75, 18987.5 , 19031.25, 19075.0 , 19118.75, 19162.5 , 19206.25, 19250.0 ,
      19293.75, 19337.5 , 19381.25, 19425.0 , 19468.75, 19512.5 , 19556.25, 19600.0 , 19643.75,
      19687.5 , 19731.25, 19775.0 , 19818.75, 19862.5 , 19906.25, 19950.0 , 19993.75, 20037.5 ,
      20081.25, 20125.0 , 20168.75, 20212.5 , 20256.25, 20300.0 , 20343.75, 20387.5 , 20431.25,
      20475.0 , 20518.75, 20562.5 , 20606.25, 20650.0 , 20693.75, 20737.5 , 20781.25, 20825.0 ,
      20868.75, 20912.5 , 20956.25, 21000.0 , 21043.75, 21087.5 , 21131.25, 21175.0 , 21218.75,
      21262.5 , 21306.25, 21350.0 , 21393.75, 21437.5 , 21481.25, 21525.0 , 21568.75, 21612.5 ,
      21656.25, 21700.0 , 21743.75, 21787.5 , 21831.25, 21875.0 , 21918.75, 21962.5 , 22006.25,
      22050.0 , 22093.75, 22137.5 , 22181.25, 22225.0 , 22268.75, 22312.5 , 22356.25, 22400.0
    )

    testClass0.frequencyVector should equal(freqVect0)
    ErrorMetrics.rmse(testClass1.frequencyVector, freqVect1) should be < 1.0E-16
  }

  it should "raise IllegalArgumentException when given a frequency higher than half of samplingRate" in {
    val testClassFail = TestClass(1024, 44800.0f)
    an[IllegalArgumentException] should be thrownBy testClassFail.frequencyToIndex(22500.0)
  }

  it should "raise IllegalArgumentException when given a negative frequency" in {
    val testClassFail = TestClass(1024, 44800.0f)
    an[IllegalArgumentException] should be thrownBy testClassFail.frequencyToIndex(-22300.0)
  }

  it should "raise IllegalArgumentException when given a negative index" in {
    val testClassFail = TestClass(1024, 44800.0f)
    an[IllegalArgumentException] should be thrownBy testClassFail.indexToFrequency(-1)
  }

  it should "raise IllegalArgumentException when given a index that exceeds spectrum size" in {
    val testClassFail = TestClass(1024, 44800.0f)
    an[IllegalArgumentException] should be thrownBy testClassFail.indexToFrequency(1030)
  }
}
