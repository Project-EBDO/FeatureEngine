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
