package bclconverter

import org.apache.flink.streaming.api.scala._
import bclconverter.{FlinkStreamProvider => FP}
import java.io.{File, BufferedInputStream, FileInputStream}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.common.io.{SerializedInputFormat, BinaryInputFormat, FileInputFormat, GenericInputFormat}
import org.apache.flink.types.{ByteValue, StringValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.DataInputView
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.io.{SerializedOutputFormat, BinaryOutputFormat, FileOutputFormat}
import org.apache.flink.core.memory.DataOutputView

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import org.apache.flink.core.io.GenericInputSplit

import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.hadoop.mapreduce.RecordWriter


object BCL {
  def toBQ(b : Byte) : (Int, Int) = {
    return(b & 0x03, (b & 0xFC) >> 2)
  }
}

case class BCL(b : Byte) {
}


class Hout extends HFileOutputFormat[Void, BCL] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, BCL] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: BCL) = {
      fileOut.writeByte(what.b)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter [Void, BCL] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}

class Hin extends HFileInputFormat[Void, BCL] {
  class RR extends RecordReader[Void, BCL] {
    def void: Void = null
    var start = -1l
    var pos = -1l
    var end = -1l
    var fileIn : FSDataInputStream = null
    var cvalue : BCL = BCL(123) //null
    def initialize(isplit: InputSplit, ta: TaskAttemptContext) = {
      val split = isplit.asInstanceOf[FileSplit]
      println(s"SPLIT size: ${split.getLength}")
      val job = ta.getConfiguration
      start = split.getStart
      pos = start
      end = start + split.getLength
      val file = split.getPath
      val fs = file.getFileSystem(job)
      fileIn = fs.open(file)
      fileIn.seek(start)
    }
    def getCurrentKey : Void = {
      void
    }
    def getProgress : Float = {
      if (start == end)
        return 1.0f

      return (fileIn.getPos - start) / (end - start).toFloat
    }
    def close = {
    }
    def getCurrentValue : BCL = {
      cvalue
    }
    def nextKeyValue : Boolean = {
      if (pos != end) {
        fileIn.readByte
        // cvalue = BCL(fileIn.readByte)
        pos += 1
        true
      }
      else
        false
    }
  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[Void, BCL] = {
    new RR
  }
}

object inBuf {
  var buffer : MappedByteBuffer = _

  def apply(filename : String) = {
    val file = new File(filename)
    val fileChannel = new RandomAccessFile(file, "r").getChannel
    buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size())
  }

  def get : Byte = {
    buffer.get
  }

  def hasRemaining : Boolean = {
    buffer.hasRemaining
  }
}

class mm(filename : String) extends GenericInputFormat[BCL] {
  /*
   override def open(split : GenericInputSplit) {
   }
   */

  inBuf(filename)

  def nextRecord(boh : BCL) : BCL = {
    if (reachedEnd)
      return null

    BCL(inBuf.get)
  }

  def reachedEnd : Boolean = {
    !(inBuf.hasRemaining)
  }
}


class BQout(fname : String) extends BinaryOutputFormat[BCL] {
  val blsize = 1024*1024
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
  val blsize = 1024*1024
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
  val fin = "/tmp/t/big"
  val fout = "/tmp/t/out"
  
  // test
  def main(args: Array[String]) {
    def void: Void = null

    // scrivi

    FP.env.setParallelism(1)

    // val d = FP.env.createInput(new FBQin(fin))//.map((void, _))
    // val d = FP.env.createInput(new mm(fin)).map((void, _))

    val job = Job.getInstance(FP.conf)
    val hin = new HadoopInputFormat(new Hin, classOf[Void], classOf[BCL], job)
    val hipath = new HadoopPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1000000) 
    val d = FP.env.createInput(hin)//.setParallelism(1)

    val hout = new HadoopOutputFormat(new Hout, job)
    val hopath = new HadoopPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    d.writeUsingOutputFormat(hout)

    /*
    d//.map(_._2)
      .writeUsingOutputFormat(new FBQout(fout))
     */

    FP.env.execute

    hout.finalizeGlobal(1)

    // leggi
  }
}


