package bclconverter

import org.apache.flink.streaming.api.scala._
import bclconverter.{FlinkStreamProvider => FP}
import java.io.{File, BufferedInputStream, FileInputStream}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.common.io.{SerializedInputFormat, BinaryInputFormat, FileInputFormat}
import org.apache.flink.types.{ByteValue, StringValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.io.{SerializedOutputFormat, BinaryOutputFormat, FileOutputFormat}
import org.apache.flink.core.memory.DataOutputView
  
case class BCL(b : Byte) {
  def toBQ : (Int, Int) = {
    return(b & 0x03, (b & 0xFC) >> 2)
  }
}

class BQout(fname : String) extends BinaryOutputFormat[BCL] {
  val blsize = 2048
  val conf = new Configuration
  setOutputFilePath(new Path(fname))
  setWriteMode(WriteMode.OVERWRITE)
  conf.setLong(BinaryOutputFormat.BLOCK_SIZE_PARAMETER_KEY, blsize)
  configure(conf)
  open(0, 1)

  def serialize(record : BCL, dataOutput : DataOutputView) = {
    dataOutput.writeByte(record.b)
  }
}

class BQin(fname : String) extends BinaryInputFormat[BCL] {
  val blsize = 2048
  val conf = new Configuration
  conf.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, blsize)
  setFilePath(fname)
  configure(conf)

  def deserialize(record : BCL, dataInput : DataInputView) : BCL = {
    BCL(dataInput.readByte)
  }
}

class FBQout(fname : String) extends FileOutputFormat[BCL](new Path(fname)) {
  setWriteMode(WriteMode.OVERWRITE)
  val bsize = 32
  var buf = new Array[Byte](bsize)
  var lpos : Int = 0 // local pos

  def writeRecord2(boh: BCL) = {
    stream.write(boh.b)
  }

  def writeRecord(boh: BCL) = {
    buf(lpos) = boh.b
    lpos += 1
    if (lpos == bsize) {
      stream.write(buf)
      lpos = 0
    }
  }

  override def close = {
    stream.write(buf, 0, lpos)
    super.close
  }

}

class FBQin(fname : String) extends FileInputFormat[BCL](new Path(fname)) {
  val bsize = 32
  var buf = new Array[Byte](bsize)
  var gpos : Long = -1 // global pos
  var lpos : Int = -1 // local pos
  var endpos : Long = -1 // end (global) pos

  override def open(fileSplit : FileInputSplit) = {
    super.open(fileSplit)
    gpos = stream.getPos
    endpos = getSplitStart + getSplitLength
  }

  def nextRecord2(boh: BCL): BCL = {
    if (stream.getPos >= splitStart + splitLength)
      return null

    val x = stream.read.toByte
    BCL(x)
  }

  def nextRecord(boh: BCL): BCL = {
    // end of filesplit
    if (gpos >= endpos)
      return null

    // read new block
    if (gpos == stream.getPos) {
      stream.read(buf)
      lpos = 0
    }

    val x = buf(lpos)
    lpos += 1
    gpos += 1
    BCL(x)
  }

  def reachedEnd : Boolean = {
    false
  }
}

object Reader {
  val fin = "/tmp/t/boh.bcl"
  val fout = "/tmp/t/out.bin"
  
  // test
  def main(args: Array[String]) {
    // scrivi

    FP.env.setParallelism(4)
    val d = FP.env.createInput(new FBQin(fin))//.rebalance
    d.writeUsingOutputFormat(new FBQout(fout))

    FP.env.execute

    // leggi
  }
}


