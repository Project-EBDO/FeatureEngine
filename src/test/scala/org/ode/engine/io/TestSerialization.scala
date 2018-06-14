
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

import org.ode.engine.signal_processing._

import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
import org.scalatest.{FlatSpec, Matchers}

class TestSerialization extends FlatSpec with Matchers {
  def serialize(obj: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close
    stream.toByteArray
  }

  def deserialization(bytes: Array[Byte]): Any = {
    val stream: ByteArrayInputStream = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(stream)
    ois.readObject
  }

  val objects: List[(String, Any)] = List(
    ("Energy", new Energy(10)),
    ("FFT", new FFT(10)),
    ("Periodogram" -> new Periodogram(10, 1.0)),
    ("Segmentation" -> new Segmentation(10, Some(5))),
    ("WelchSpectralDensity" -> new WelchSpectralDensity(10)),
    ("HammingWindow" -> new HammingWindow(10, "symmetric"))
  )

  for (obj <- objects) {
    it should s"serialize an instance of ${obj._1}" in {
      val bytes = serialize(obj._2)
      val objDeserialized = deserialization(bytes)

      bytes.length should be > 0
      objDeserialized should be(obj._2)
    }
  }

}
