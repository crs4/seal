
/* Copyright (C) 2011-2016 CRS4.
 *
 * This file is part of Seal.
 *
 * Seal is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * Seal is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with Seal.  If not, see <http://www.gnu.org/licenses/>.
 */


package bclconverter.bclreader

import annotation.tailrec
import java.io._
import com.google.common.primitives.Longs

object CreateTable {
  val toNum = Array("00", "01", "02", "03")
  val toHex = Array("41", "43", "47", "54")  // A C G T
  val toBase : Array[Byte] = Array('A', 'C', 'G', 'T')
  def addEl(in : Array[Int], el : Int) : Array[Int] = {
    in :+ el
  }
  def addDim(in : Array[Array[Int]]) : Array[Array[Int]] = {
    in.flatMap(x => Range(0,4).map(e => addEl(x, e)))
  }
  @tailrec
  def iterDim(in : Array[Array[Int]], iter : Int) : Array[Array[Int]] = {
    if (iter == 0)
      in
    else {
      val nin = addDim(in)
      iterDim(nin, iter - 1)
    }
  }
  def getLine(in : Array[Int]) : String = {
    in.map(toHex).mkString("0x", "", "l")
  }
  def getArray : Array[Long] = {
    val zero = Array[Int]()
    val s = iterDim(Array(zero), 8)
    s.map(_.map(toBase(_))).map(Longs.fromByteArray(_))
  }
  def main(args: Array[String]) {
    val zero = Array[Int]()
    val s = iterDim(Array(zero), 8).map(getLine)
    val os = s.mkString("val toOctet = Array(\n", " ,\n", "\n)")
    // println(os)
    val out = new FileWriter(new File("/tmp/table.scala"))
    out.write(os)
    out.close
  }
}
