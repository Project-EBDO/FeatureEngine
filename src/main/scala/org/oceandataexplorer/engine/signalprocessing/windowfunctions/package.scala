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

package org.oceandataexplorer.engine.signalprocessing

/**
 * Window Functions package object providing types for window functions.
 */
package object windowfunctions {
  /**
   * Enumeration providing types for window functions
   */
  object WindowFunctionTypes {
    /**
     * The trait for the window functions types
     */
    sealed trait WindowFunctionType

    /**
     * The available types for window functions:
     * - symmetric is usually used for filter design
     */
    case object Symmetric extends WindowFunctionType

    /**
     * - periodic is usually used for spectral analysis
     */
    case object Periodic extends WindowFunctionType
  }
}
