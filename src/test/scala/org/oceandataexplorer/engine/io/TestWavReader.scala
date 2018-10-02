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

package org.oceandataexplorer.engine.io

import java.io.File

import org.oceandataexplorer.utils.test.OdeCustomMatchers
import org.scalatest.{FlatSpec, Matchers}


/**
 * Tests for WavReader
 *
 * @author Joseph Allemandou
 */
class TestWavReader extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 0.001

  private val soundFilePath1 = "/wav/sin_16kHz_2.5s.wav"
  private val expectedFirstWave = Array(
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
    chunks should have size math.ceil(2.5 * 16000 / chunkSize).toLong
    chunks.head should have length 1 // single channel

    // Check the first wave of sin signal
    val firstWave = chunks.head.head

    firstWave should rmseMatch(expectedFirstWave)

    // Use repetitive aspect of signal to check reading correctness
    Range.Int(1, math.floor(2.5 * 16000 / chunkSize).toInt, 1).foreach((chunkIdx: Int) => {
      firstWave should rmseMatch(chunks(chunkIdx).head)
    })

  }

  it should "Correctly read an example file partially" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16 // Length of sin signal without repetition
    val nbChunks = 10

    val chunks = wavReader.readChunks(chunkSize, 0, nbChunks)
    chunks should have size nbChunks
    chunks.head should have length 1 // single channel

    // Check the first wave of sin signal
    val firstWave = chunks.head.head

    firstWave should rmseMatch(expectedFirstWave)

    // Use repetitive aspect of signal to check reading correctness
    Range.Int(1, nbChunks, 1).foreach((chunkIdx: Int) => {
      firstWave should rmseMatch(chunks(chunkIdx).head)
    })

  }

  it should "Correctly read an example file partially not from begining" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16 // Length of sin signal without repetition
    val offset = 1024
    val nbChunks = 10

    val chunks = wavReader.readChunks(chunkSize, offset, nbChunks)
    chunks should have size nbChunks
    chunks.head should have length 1 // single channel

    // Check the first wave of sin signal
    val firstWave = chunks.head.head

    firstWave should rmseMatch(expectedFirstWave)

    // Use repetitive aspect of signal to check reading correctness
    Range.Int(1, nbChunks, 1).foreach((chunkIdx: Int) => {
      firstWave should rmseMatch(chunks(chunkIdx).head)
    })
  }

  it should "raise an IllegalArgumentException when given an unsupported file format" in {
    val file: File = new File(getClass.getResource("/wav/aiffFormated.wav").toURI)

    the[IllegalArgumentException] thrownBy {
      new WavReader(file)
    } should have message "Input file is not wav"
  }

  it should "raise an IllegalArgumentException when the given wav file has big endianness" in {
    val file: File = new File(getClass.getResource("/wav/bigEndian.wav").toURI)

    an[javax.sound.sampled.UnsupportedAudioFileException] should be thrownBy {
      new WavReader(file)
    }
  }

  it should "raise an IllegalArgumentException when the given wav file encoding is not supported" in {
    val file: File = new File(getClass.getResource("/wav/floatEncoding.wav").toURI)

    the[IllegalArgumentException] thrownBy {
      new WavReader(file)
    } should have message "Input file is not integer-PCM formatted"
  }

  it should "raise an IllegalArgumentException when the given wav file is corrupted" in {
    val file: File = new File(getClass.getResource("/wav/sin_16kHz_2.5s_corrupted.wav").toURI)

    the[IllegalArgumentException] thrownBy {
      new WavReader(file)
    } should have message "Input file length doesn't match computed one - probably corrupted"
  }

  it should "raise an IllegalArgumentException when given chunkSize is larger than authorized" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = Int.MaxValue
    val offset = 1024
    val nbChunks = 10

    the[IllegalArgumentException] thrownBy {
      wavReader.readChunks(chunkSize, offset, nbChunks)
    } should have message "chunkSize larger than authorized"
  }

  it should "raise an IllegalArgumentException when given offsetFrame is larger than file's number of frames" in {
    val file: File = new File(getClass.getResource(soundFilePath1).toURI)
    val wavReader = new WavReader(file)

    val chunkSize = 16
    val offset = 10000000
    val nbChunks = 10

    the[IllegalArgumentException] thrownBy {
      wavReader.readChunks(chunkSize, offset, nbChunks)
    } should have message "offsetFrame larger than file's number of frames"
  }
}
