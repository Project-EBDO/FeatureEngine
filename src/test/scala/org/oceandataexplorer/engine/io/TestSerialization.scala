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

import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.workflows._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import WindowFunctionTypes.{Symmetric, Periodic}

import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.scalatest.{FlatSpec, Matchers}


/**
 * Test for Serialization of core objects
 *
 * @author Alexandre Degurse
 */
class TestSerialization extends FlatSpec with Matchers {

  def serialize(obj: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close()
    stream.toByteArray
  }

  def deserialize(bytes: Array[Byte]): Any = {
    val stream: ByteArrayInputStream = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(stream)
    ois.readObject
  }

  private val serializableObjects: List[(String, Any)] = List(
    "Segmentation" -> new Segmentation(10, 5),
    "HammingWindow" -> new HammingWindowFunction(10, Symmetric),
    "FFT" -> new FFT(10, 1.0f),
    "Periodogram" -> new Periodogram(10, 1.0, 1.0f),
    "WelchSpectralDensity" -> new WelchSpectralDensity(10, 1.0f),
    "Energy" -> new Energy(10),
    "TOL" -> new TOL(10, 1.0f)
  )

  for ((objName, obj) <- serializableObjects) {
    it should s"serialize an instance of $objName" in {
      val bytes = serialize(obj)
      val objDeserialized = deserialize(bytes)

      bytes.length should be > 0
      objDeserialized should equal(obj)
    }
  }
}
