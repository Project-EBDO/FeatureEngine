/** Copyright (C) 2017 Project-EBDO
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

package org.ebdo.engine.signal_processing;

import scala.collection.mutable
import scala.math.{cos,Pi}

/**
  * Spectrogram Windows Functions for signal processing
  * Author: Alexandre Degurse, Paul NGuyenhongduc
  *
  * Relies on several part of open source codes by JorenSix :
  * https://github.com/JorenSix/TarsosDSP/tree/master/src/core/be/tarsos/dsp/util/fft
  */

/**
* SpectrogramWindows class provides functions to apply and store spectoram windows
* @param windowsToCompute the list of spectoram windows to be precomputed,
*   it is a sequence of tuples that contains the name of the
*   window and its size
*/
class SpectrogramWindows(windowsToCompute: Seq[(String, Int)]) {
  /**
  * Constructor overload called when a single window is provided
  * @param window the spectoram window to be precomputed
  *   tuples that contains the name of the window and its size
  */
  def this(window: (String, Int)) = this(Seq(window))

  // Map that stores the windows that have been computes
  private var windows: mutable.Map[(String,Int), Window] = mutable.Map() ++
    windowsToCompute.map(x => (x, Window(x._1, x._2))).toMap

  /**
  * Function that generate a new window and stores it the windows
  * @param winType the type of the window to be compute (e.g. "hamming")
  * @param size the size of the window to be compute
  * @return Unit
  */
  def generateWindow(winType: String, size: Int) {
    windows += ((winType, size) -> Window(winType, size))
  }

  /**
  * Function that applies a window to a signal
  * @param signal the signal to window
  * @param winType the type of window to be applied
  * @return the signal windowed with the specified window
  */
  def applyWindow(signal: Seq[Double], winType: String) : Seq[Double] = {
    // see if the asked window has already been computed and stored in windows
    if (!windows.contains((winType,signal.length))) {
      // if not, compute it and store it
      generateWindow(winType,signal.length)
    }
    // apply the window to the signal and return the result
    return windows((winType,signal.length)).applyWindow(signal)
  }

  // getter that return the list of windows that are stored in the Map windows
  def listOfWindows: Seq[(String, Int)] = windows.keys.toSeq

  def getWindow(winType: String, size: Int): Seq[Double] = {
    if (!windows.contains((winType, size)))
      generateWindow(winType, size)

    return windows((winType, size)).window
  }
}


/**
* abstract class Window, provides generic functions and definition common to all windows
* @param size the size of the windows to be generated
*/
abstract class Window(winSize: Int) {
  // the size of this window
  val size = winSize

  // abstract method used to generate this window
  def windowMethod: (Int, Int) => Double

  // function that generate this window with windowMethod
  def generateWindow(winSize: Int): Seq[Double] = {
    return Seq.tabulate(size)(i => windowMethod(i, size))
  }

  // sequence that contain the computed window
  val window: Seq[Double] = generateWindow(winSize)

  /**
  * Function that applies this window to a signal
  * @param signal the signal to be windowed
  * @return the windowed signal
  */
  def applyWindow(signal: Seq[Double]): Seq[Double] = {
    return signal.zip(window)
      .map(x => x._1 * x._2)
      .toSeq
  }
}

trait Hamming {
  /**
  * Function that computes a coefficient for Hamming window
  *
  * @param i coefficient to compute
  * @param N length of the window
  * @return one Hamming coefficient
  */
  def hamming(i: Int, N: Int): Double = {
    // Generate the i-th coefficient of a N-point Hamming window
    return 0.54 - 0.46 * cos(2 * Pi * i / (N - 1))
  }
}

class HammingWindow(size: Int) extends Window(size) with Hamming {
  override def windowMethod = hamming
}

object Window {
  def apply(name: String, size: Int) : Window = {
    val win : Window = name match {
      case "hamming" => new HammingWindow(size)
      case _ => throw new IllegalArgumentException("Specified window type not found !")
    }

    return win
  }
}
