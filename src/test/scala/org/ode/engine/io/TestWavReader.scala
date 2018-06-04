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

package org.ode.engine.io

import org.ode.utils.test.ErrorMetrics.rmse

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import javax.sound.sampled.{AudioFileFormat, AudioFormat, AudioInputStream, AudioSystem}

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration
import scala.io.Source

class TestWavReader extends FlatSpec with Matchers {


  val soundFilePath1 = "/wav/sin_16kHz_2.5s.wav"
  val maxRMSEDiff = 0.001
  val expectedFirstWave = Seq(
    0.0d,
    0.3826904296875d,
    0.707122802734375d,
    0.923858642578125d,
    0.999969482421875d,
    0.92388916015625d,
    0.707061767578125d,
    0.382781982421875d,
    -9.1552734375E-5d,
    -0.38262939453125d,
    -0.707122802734375d,
    -0.923919677734375d,
    -0.99993896484375d,
    -0.9239501953125d,
    -0.70703125d,
    -0.382720947265625d
  )


  "TestWavReader" should "Correctly read a example file" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16 // Length of sin signal without repetition

    val chunks = wavReader.readChunks(chunkSize, 0)
    chunks.size should equal(math.ceil(2.5 * 16000 / chunkSize))
    chunks(0).size should equal(1) // single channel

    // Chek the first wave of sin signal
    val firstWave = chunks(0)(0).toSeq
    rmse(firstWave, expectedFirstWave) should be < maxRMSEDiff

    // Use repetitve aspect of signal to check reading correctness
    Range.Int(1, math.floor(2.5 * 16000 / chunkSize).toInt, 1).foreach((chunkIdx: Int) => {
      rmse(firstWave, chunks(chunkIdx)(0).toSeq) should be < maxRMSEDiff
    })

  }

  it should "Correctly read an example file partially" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16 // Length of sin signal without repetition
    val nbChunks = 10

    val chunks = wavReader.readChunks(chunkSize, 0, nbChunks)
    chunks.size should equal(nbChunks)
    chunks(0).size should equal(1) // single channel

    // Chek the first wave of sin signal
    val firstWave = chunks(0)(0).toSeq
    rmse(firstWave, expectedFirstWave) should be < maxRMSEDiff

    // Use repetitve aspect of signal to check reading correctness
    Range.Int(1, nbChunks, 1).foreach((chunkIdx: Int) => {
      rmse(firstWave, chunks(chunkIdx)(0).toSeq) should be < maxRMSEDiff
    })

  }

  it should "Correctly read an example file partially not from begining" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16 // Length of sin signal without repetition
    val offset = 1024
    val nbChunks = 10

    val chunks = wavReader.readChunks(chunkSize, offset, nbChunks)
    chunks.size should equal(nbChunks)
    chunks(0).size should equal(1) // single channel

    // Chek the first wave of sin signal
    val firstWave = chunks(0)(0).toSeq
    rmse(firstWave, expectedFirstWave) should be < maxRMSEDiff

    // Use repetitve aspect of signal to check reading correctness
    Range.Int(1, nbChunks, 1).foreach((chunkIdx: Int) => {
      rmse(firstWave, chunks(chunkIdx)(0).toSeq) should be < maxRMSEDiff
    })

  }

}