package org.ode.engine.tmp

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import javax.sound.sampled.{AudioFileFormat, AudioFormat, AudioInputStream, AudioSystem}

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class TestReadingFiles extends FlatSpec with Matchers {

  val mdFilePath = "/wav/tutorial.md"
  val soundFilePath1 = "/wav/whitenoise_16kHz_2.5s.wav"
  val soundFilePath2 = "/wav/sin_48kHz_10s.wav"
  val soundFilePathCorrupted = "/wav/sin_16kHz_2.5s_corrupted.wav"

  "Tutorial file" should "be readable" in {
    val tutorialUrl: URL = getClass.getResource(mdFilePath)
    val tutorialFile: File = new File(tutorialUrl.toURI)
    val tutorialFileInputStream: InputStream = new FileInputStream(tutorialFile)
    val tutorialResourceInputStream: InputStream = getClass.getResourceAsStream(mdFilePath)
    Source.fromInputStream(tutorialFileInputStream).mkString should be(
      Source.fromInputStream(tutorialResourceInputStream).mkString)
  }

  "Example sound file 1 " should "be readable by javax.sound" in {
    val freq = 16000.0
    val chans = 1
    val bytes = 2.0
    val duration = 2.5
    val dataBytes = freq * duration * chans * bytes

    val inputStream : InputStream = getClass.getResourceAsStream(soundFilePath1)

    val audioFileFormat: AudioFileFormat = AudioSystem.getAudioFileFormat(inputStream)
    audioFileFormat.getByteLength should be(dataBytes + 44)
    audioFileFormat.getFrameLength should be(duration * freq)
    audioFileFormat.getType should be(AudioFileFormat.Type.WAVE)

    val audioFormat: AudioFormat = audioFileFormat.getFormat
    audioFormat.getChannels should be(chans)             // We expect mono, not stereo
    audioFormat.getEncoding should be(AudioFormat.Encoding.PCM_SIGNED)
    audioFormat.getFrameRate should be(freq)           // frameRate defined per file
    audioFormat.getFrameSize should be(bytes)          // 2 bytes of data per frame
    audioFormat.getSampleRate should be(freq)          // Equals frameRate for PCM
    audioFormat.getSampleSizeInBits should be(8 * bytes)
    audioFormat.isBigEndian should be(false)           // Default encoding is little-endian


    val audioInputStream: AudioInputStream = AudioSystem.getAudioInputStream(inputStream)
    val arraySize = 98
    var byteArray: Array[Byte] = new Array[Byte](arraySize)
    (1 to (dataBytes / arraySize).toInt).foreach(halfSec => {
      audioInputStream.read(byteArray) should be(arraySize)
    })
    audioInputStream.read(byteArray) should be(dataBytes % arraySize)

  }

  "Example sound file 2 " should "be readable by javax.sound" in {
    val freq = 48000.0
    val chans = 1
    val bytes = 2.0
    val duration = 10
    val dataBytes = freq * duration * chans * bytes

    val inputStream : InputStream = getClass.getResourceAsStream(soundFilePath2)

    val audioFileFormat: AudioFileFormat = AudioSystem.getAudioFileFormat(inputStream)
    audioFileFormat.getByteLength should be(dataBytes + 44)
    audioFileFormat.getFrameLength should be(duration * freq)
    audioFileFormat.getType should be(AudioFileFormat.Type.WAVE)

    val audioFormat: AudioFormat = audioFileFormat.getFormat
    audioFormat.getChannels should be(chans)             // We expect mono, not stereo
    audioFormat.getEncoding should be(AudioFormat.Encoding.PCM_SIGNED)
    audioFormat.getFrameRate should be(freq)           // frameRate defined per file
    audioFormat.getFrameSize should be(bytes)          // 2 bytes of data per frame
    audioFormat.getSampleRate should be(freq)          // Equals frameRate for PCM
    audioFormat.getSampleSizeInBits should be(8 * bytes)
    audioFormat.isBigEndian should be(false)           // Default encoding is little-endian


    val audioInputStream: AudioInputStream = AudioSystem.getAudioInputStream(inputStream)
    val arraySize = 98
    var byteArray: Array[Byte] = new Array[Byte](arraySize)
    (1 to (dataBytes / arraySize).toInt).foreach(halfSec => {
      audioInputStream.read(byteArray) should be(arraySize)
    })
    audioInputStream.read(byteArray) should be(dataBytes % arraySize)

  }

  "Example sound file corrupted" should "fail to be read by javax.sound" in {
    val freq = 16000.0
    val chans = 1
    val bytes = 2.0
    val duration = 2.5
    val dataBytes = freq * duration * chans * bytes

    val audioFile = new File(getClass.getResource(soundFilePathCorrupted).toURI)

    val audioFileFormat: AudioFileFormat = AudioSystem.getAudioFileFormat(audioFile)
    audioFileFormat.getByteLength should be(freq * bytes * chans * duration + 44)
    audioFileFormat.getFrameLength should be(duration * freq)
    audioFileFormat.getType should be(AudioFileFormat.Type.WAVE)

    val audioFormat: AudioFormat = audioFileFormat.getFormat
    audioFormat.getChannels should be(chans)             // We expect mono, not stereo
    audioFormat.getEncoding should be(AudioFormat.Encoding.PCM_SIGNED)
    audioFormat.getFrameRate should be(freq)           // frameRate defined per file
    audioFormat.getFrameSize should be(bytes)          // 2 bytes of data per frame
    audioFormat.getSampleRate should be(freq)          // Equals frameRate for PCM
    audioFormat.getSampleSizeInBits should be(8 * bytes)
    audioFormat.isBigEndian should be(false)           // Default encoding is little-endian

    // Corrupted file - real length should not match computed one
    audioFile.length should not be(dataBytes + 44)
  }

}