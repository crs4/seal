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
