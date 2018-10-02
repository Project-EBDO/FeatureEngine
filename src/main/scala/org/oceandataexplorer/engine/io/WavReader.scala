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

import java.io.{FileInputStream, BufferedInputStream, File}
import javax.sound.sampled.{AudioInputStream, AudioFormat, AudioSystem, AudioFileFormat}


/**
 * Wave-file reader by chunks, using javax.sound to get file format.
 *
 * @author Joseph Allemandou
 *
 * @param audioFile The file to read as a wav file
 */
class WavReader(audioFile: File) {

  private val acceptedEncodings = Seq(
    AudioFormat.Encoding.PCM_SIGNED, AudioFormat.Encoding.PCM_UNSIGNED
  )
  private val audioFileFormat: AudioFileFormat = AudioSystem.getAudioFileFormat(audioFile)
  private val audioFormat: AudioFormat = audioFileFormat.getFormat
  // Calculate the scaling factor for converting to a normalised double
  private val (doubleOffset: Double, doubleScale: Double) =
    if (audioFormat.getSampleSizeInBits > 8) {
      (0.0, (1 << (audioFormat.getSampleSizeInBits - 1)).toDouble)
    }
    else {
      (-1.0, 0.5 * ((1 << audioFormat.getSampleSizeInBits) - 1))
    }

  /** Check file format and length correctness
   * big-endian formatted sound file are not supported, an exception will be thrown
   * by AudioFileFormat when such sound file is encountered
   */
  if (audioFileFormat.getType != AudioFileFormat.Type.WAVE) {
    throw new IllegalArgumentException("Input file is not wav")
  }
  if (! acceptedEncodings.contains(audioFormat.getEncoding)) {
    throw new IllegalArgumentException("Input file is not integer-PCM formatted")
  }
  if (audioFile.length != audioFileFormat.getByteLength) {
    throw new IllegalArgumentException(
      "Input file length doesn't match computed one - probably corrupted"
    )
  }


  /**
   *Read the wav content by chunks
   *
   * @param chunkSize The number of frames to load in a chunk. The number of sound pressure values
   *                  loaded is chunkFrames * nbChannels. This value is not to be larger than
   *                  Int.MaxValue / nbChannels for buffer size.
   * @param offsetFrame The frame number where to start to read (Optional, defaults to 0)
   * @param nbChunks The maximum number of chunks to read (Optional, defaults Long.MaxValue)
   * @return A sequence of chunks, being an array of arrays: [channel][signal]
   */
  def readChunks(
    chunkSize: Int,
    offsetFrame: Long = 0,
    nbChunks: Long = Long.MaxValue
  ): Seq[Array[Array[Double]]] = {
    if (offsetFrame >= audioFileFormat.getFrameLength) {
      throw new IllegalArgumentException("offsetFrame larger than file's number of frames")
    }
    if (chunkSize >= Int.MaxValue / audioFormat.getChannels) {
      throw new IllegalArgumentException("chunkSize larger than authorized")
    }

    // Indices
    val nbAvailableFrames: Long = audioFileFormat.getFrameLength - offsetFrame
    val nbAvailableChunks: Long = math.ceil(nbAvailableFrames.toDouble / chunkSize).toLong

    val chunksToRead: Long = scala.math.min(nbAvailableChunks, nbChunks)
    val nbBytesInChunk: Int = chunkSize * audioFormat.getFrameSize
    val nbBytesInSample: Int = audioFormat.getSampleSizeInBits / 8

    val bytesToDrop: Long = offsetFrame * audioFormat.getFrameSize

    // Do the reading iteratively using an Audio input stream
    // on top of a buffered input stream (audio stream starts
    // reading after header)
    val audioIS: AudioInputStream = AudioSystem.getAudioInputStream(
      new BufferedInputStream(new FileInputStream(audioFile)))
    audioIS.skip(bytesToDrop)

    val res = readLoop(audioIS, chunksToRead, nbBytesInChunk, nbBytesInSample)

    audioIS.close()

    res
  }

  private def readLoop(
    audioIS: AudioInputStream,
    chunksToRead: Long,
    nbBytesInChunk: Int,
    nbBytesInSample: Int
  ): Seq[Array[Array[Double]]] ={
    // Read using vars for efficiency
    val readBuffer = new Array[Byte](nbBytesInChunk)
    Range.Long(0L, chunksToRead, 1L)
      .foldLeft(Seq.empty[Array[Array[Double]]])((res, _) => {
        val nbBytesRead: Int = audioIS.read(readBuffer)
        val nbFramesRead: Int = nbBytesRead /audioFormat.getFrameSize
        // Enforce frame completeness
        assert(nbBytesRead % audioFormat.getFrameSize == 0)

        val chunk: Array[Array[Double]] =
          Array.ofDim(audioFormat.getChannels, nbFramesRead)

        Range(0, nbFramesRead, 1).foreach((frameIdx: Int) => {
          Range(0, audioFormat.getChannels, 1).foreach((channelIdx: Int) => {
            val extractedLong = Range(0, nbBytesInSample, 1)
              .foldLeft(0L)((l, byteIdx) => {
                val readInt: Int = {
                  val currentIDx = frameIdx * audioFormat.getFrameSize +
                    channelIdx * nbBytesInSample + byteIdx
                  if (byteIdx < nbBytesInSample - 1 || nbBytesInSample == 1) {
                    readBuffer(currentIDx) & 0xFF
                  } else {
                    readBuffer(currentIDx)
                  }
                }
                l + (readInt << (byteIdx * 8))
            })
            val scaledDouble: Double = doubleOffset + extractedLong.toDouble / doubleScale
            chunk(channelIdx)(frameIdx) = scaledDouble
          })
        })
        res :+ chunk
    })
  }

}
