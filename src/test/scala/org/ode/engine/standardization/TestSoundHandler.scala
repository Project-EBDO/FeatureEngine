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

package org.ode.engine.standardization


import org.scalatest.{FlatSpec, Matchers}

/**
 * Some tests for SoundHandler class
 */

class TestSoundHandler extends FlatSpec with Matchers {

  it should "read a wav file when given its parameters" in {
    val sp = SoundHandler("Sound1", 64, 24, 9811, 3906.0, 1)

    val expectedParametersString = "Sound1_64_24_9811_3906.0_1"
    val expectedSoundLength = 9811

    sp.soundParametersString should equal(expectedParametersString)

    val signalRead = sp.readSound()

    signalRead.length should equal(expectedSoundLength)
  }
}
