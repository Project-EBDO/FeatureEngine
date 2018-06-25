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

package org.ode.engine.signal_processing

import org.ode.utils.test.ErrorMetrics.rmse;
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for TOL class
  *
  * @author Alexandre Degurse
  */


class TestTOL extends FlatSpec with Matchers {

  private val maxRMSE = 3.0E-14

  private val psd128 = Array(
    4.1602500000000000e+03, 8.3018982314637901e+02, 2.0767253111595070e+02,
    9.2391640473272219e+01, 5.2043434459908696e+01, 3.3368095319152424e+01,
    2.3223590267237746e+01, 1.7106902885582432e+01, 1.3137071184544086e+01,
    1.0415505025764748e+01, 8.4689253698497744e+00, 7.0288200600409816e+00,
    5.9336480124593187e+00, 5.0814952804032201e+00, 4.4054892166379576e+00,
    3.8602754069873511e+00, 3.4142135623730945e+00, 3.0446864506055240e+00,
    2.7351795158150223e+00, 2.4734065785976029e+00, 2.2500743071156775e+00,
    2.0580482561886715e+00, 1.8917779545958380e+00, 1.7468929996990219e+00,
    1.6199144044217739e+00, 1.5080450889126371e+00, 1.4090156545183423e+00,
    1.3209693768284887e+00, 1.2423754209351645e+00, 1.1719626332156958e+00,
    1.1086685176760165e+00, 1.0515995439451622e+00, 1.0000000000000000e+00,
    9.5322735079138210e-01, 9.1073259539451312e-01, 8.7204449718226218e-01,
    8.3675683885799723e-01, 8.0451805762327444e-01, 7.7502276639280554e-01,
    7.4800477949818067e-01, 7.2323134608584516e-01, 7.0049835876883393e-01,
    6.7962635431497331e-01, 6.6045716107122188e-01, 6.4285107722770385e-01,
    6.2668448697361989e-01, 6.1184783961696443e-01, 5.9824393097252226e-01,
    5.8578643762690530e-01, 5.7439866371671622e-01, 5.6401246710270447e-01,
    5.5456733767016053e-01, 5.4600960522778752e-01, 5.3829175834238596e-01,
    5.3137185861294145e-01, 5.2521303749535786e-01, 5.1978306494829041e-01,
    5.1505398096938670e-01, 5.1100178259948836e-01, 5.0760616024666172e-01,
    5.0485027826763218e-01, 5.0272059567891592e-01, 5.0120672368413632e-01,
    5.0030131742372386e-01, 2.5000000000000000e-01
  )

  it should "compute Third Octave Band Boundaries when studying default frequency range" in {
    val nfft = 128
    val samplingRate = 128.0f

    val tolClass = new TOL(nfft, samplingRate)

    val expectedBoudaries: Array[(Double, Double)] = Array(
      (0.8912509381337456,1.1220184543019633),
      (1.1220184543019638,1.4125375446227546),
      (1.4125375446227548,1.7782794100389232),
      (1.778279410038922 ,2.2387211385683385),
      (2.238721138568339 ,2.818382931264453),
      (2.818382931264454 ,3.5481338923357546),
      (3.5481338923357555,4.466835921509632),
      (4.466835921509633 ,5.623413251903492),
      (5.623413251903489 ,7.079457843841377),
      (7.079457843841379 ,8.912509381337454),
      (8.912509381337456 , 11.220184543019634),
      (11.220184543019638, 14.125375446227544),
      (14.125375446227544, 17.782794100389225),
      (17.78279410038923 , 22.387211385683397),
      (22.38721138568339 , 28.18382931264453),
      (28.183829312644537, 35.48133892335754),
      (35.481338923357555, 44.66835921509632),
      (44.66835921509631 , 56.2341325190349)
    )
    tolClass.thirdOctaveBandBounds.length should be(expectedBoudaries.length)

    val zippedResults: Array[(Array[Double], Array[Double])] =
      tolClass.thirdOctaveBandBounds.zip(expectedBoudaries)
      .map(bound => (bound._1.productIterator.toArray.map(_.asInstanceOf[Double]),
        bound._2.productIterator.toArray.map(_.asInstanceOf[Double])))


    zippedResults.foreach(
        bound => rmse(bound._1, bound._2) should be < maxRMSE
      )
  }

  it should "compute Third Octabe Levels when studying default frequency range" in {
    val nfft = 128
    val samplingRate = 32.0f

    val tolClass = new TOL(nfft, samplingRate)

    // little re-normalization since psd128 was normalized in density with samplingRate of 128.0
    val tols = tolClass.compute(psd128.map(_ * 4.0))

    val expectedTols = Array(
      25.67692669684795, 23.184259402046305, 23.548126209403705, 18.352313812634517, 21.075016847040825,
      18.58391927707086, 16.695020229990888, 17.0109786752842, 15.796288449641548, 14.735492156642533,
      14.345791328130584, 14.408636676307502
    )

    rmse(tols, expectedTols) should be < maxRMSE
  }

  it should "compute Third Octave Band Boundaries when studying custom frequency range" in {
    val nfft = 128
    val samplingRate = 128.0f
    val lowFreq = Some(35.2)
    val highFreq = Some(50.5)

    val tolClass = new TOL(nfft, samplingRate, lowFreq, highFreq)

    val expectedBoudaries: Array[(Double, Double)] = Array(
      (28.183829312644537, 35.48133892335754),
      (35.481338923357555, 44.66835921509632),
      (44.66835921509631 , 56.2341325190349 )
    )

    tolClass.thirdOctaveBandBounds.foreach(println)
    tolClass.thirdOctaveBandBounds.length should be(expectedBoudaries.length)

    val zippedResults: Array[(Array[Double], Array[Double])] =
      tolClass.thirdOctaveBandBounds.zip(expectedBoudaries)
      .map(bound => (bound._1.productIterator.toArray.map(_.asInstanceOf[Double]),
        bound._2.productIterator.toArray.map(_.asInstanceOf[Double])))


    zippedResults.foreach(
        bound => rmse(bound._1, bound._2) should be < maxRMSE
      )
  }

  it should "compute Third Octave Levels when studying custom frequency range" in {
    val nfft = 128
    val samplingRate = 128.0f
    val lowFreq = Some(35.2)
    val highFreq = Some(50.5)

    val tolClass = new TOL(nfft, samplingRate, lowFreq, highFreq)

    val tols = tolClass.compute(psd128)

    val expectedTols = Array(
      8.714892243362911, 8.325191414850961, 8.388036763027877
    )

    rmse(tols, expectedTols) should be < maxRMSE
  }

  it should "raise IllegalArgumentException when given a mishaped PSD" in {
    val tolClass = new TOL(100, 100.0f)

    an[IllegalArgumentException] should be thrownBy tolClass.compute(Array(1.0))
  }

  it should "raise IllegalArgumentException when given windows that are smaller than 1 second" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 1000.0f)
  }

  it should "raise IllegalArgumentException when given low frequency is higher than sampling rate / 2" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 100.0f, Some(200.0))
  }

  it should "raise IllegalArgumentException when given high frequency is higher than sampling rate / 2" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 100.0f,  Some(100.0))
  }

  it should "raise IllegalArgumentException when given high frequency is smaller than 25 Hz" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 100.0f, Some(25.0), Some(0.0))
  }

  it should "raise IllegalArgumentException when given low frequency is smaller than 25 Hz" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 100.0f,  Some(0.0))
  }

  it should "raise IllegalArgumentException when given low frequency is higher than high frequency" in {
    an[IllegalArgumentException] should be thrownBy new TOL(100, 100.0f,  Some(40.0), Some(30.0))
  }
}
