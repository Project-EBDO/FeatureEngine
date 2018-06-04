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

import org.ode.utils.test.ErrorMetrics
import org.ode.utils.test.ErrorMetrics.rmse

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import javax.sound.sampled.{AudioFileFormat, AudioFormat, AudioInputStream, AudioSystem}

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration
import scala.io.Source

class TestWindow(val windowSize: Int) extends SpectrogramWindow {
  val windowCoefficients: Array[Double] =
    (1 to windowSize).map(_.toDouble).toArray
}

class TestSpectrogramWindow extends FlatSpec with Matchers {

  val testWindowSize = 1024
  val testWindow = new TestWindow(testWindowSize)
  val testSignal = (1 to testWindowSize).map(1.0 / _.toDouble).toArray

  it should "fail applying with mutables when signal doesn't match window size" in {
    an [IllegalArgumentException] should be thrownBy testWindow.applyToSignal(testSignal.take(10))
  }

  it should "apply with mutables" in {
    testWindow.applyToSignal(testSignal).sum should equal(testWindowSize)
  }
}
