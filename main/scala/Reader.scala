package bclconverter.hadoop

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
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import org.apache.flink.core.io.GenericInputSplit

import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType

object BCL {
  type Robo = Long
  val rsize = 8
  def toBQ(b : Robo) : (Long, Long) = {
    return(b & 0x03, (b & 0xFC) >> 2)
  }
}

import BCL.{Robo, rsize}

case class BCL(b : Robo) {
}

class Hout extends HFileOutputFormat[Void, BCL] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, BCL] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: BCL) = {
      fileOut.writeLong(what.b)
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
    var cvalue : BCL = null

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
        /////////////// FIX ME: read long, not byte
        cvalue = BCL(fileIn.readLong)
        pos += 8 // 64bit long
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

class BHout extends HFileOutputFormat[Void, BCL] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, BCL] {
    val bsize = 1024
    var buf = new Array[Byte](bsize)
    var lpos : Int = 0 // local pos
    def close(ta : TaskAttemptContext) = {
      fileOut.write(buf, 0, lpos)
      fileOut.close
    }
    def write(void: Void, what: BCL) = {
      val mini = ByteBuffer.allocate(rsize)
      mini.putLong(what.b)
      mini.array.copyToArray(buf, lpos)
      lpos += rsize
      if (lpos == bsize) {
        fileOut.write(buf)
        lpos = 0
      }
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


class BHin extends HFileInputFormat[Void, BCL] {
  class RR extends RecordReader[Void, BCL] {
    def void: Void = null
    var start = -1l
    var pos = -1l
    var end = -1l
    var fileIn : FSDataInputStream = null
    var cvalue : BCL = null
    val bsize = 1024
    var lpos = bsize
    var buf = new Array[Byte](bsize)

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

    def readNewBlock : Boolean = {
      if (pos != end) {
        val r = fileIn.read(buf)
        pos += r
        lpos = 0
        return (r > 0)
      }
      else
        false
    }

    def nextKeyValue : Boolean = {
      var ok = true
      if (lpos == buf.length)
        ok = readNewBlock

      if (!ok)
        return false

      val mini = ByteBuffer.wrap(buf.slice(lpos, lpos + rsize))
      cvalue = BCL(mini.getLong)
      lpos += rsize // 64 bit long
      return true
    }
  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[Void, BCL] = {
    new RR
  }
}




object HReader {
  val fin = "/tmp/t/huge"
  val fout = "/tmp/t/out"
  
  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(4)

    // val dd = FP.env.createInput(new FBQin(fin)).map(x => (void, x))

    val job = Job.getInstance(FP.conf)

    val hin = new HadoopInputFormat(new Hin, classOf[Void], classOf[BCL], job)
    val hipath = new HPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1024*1024)
    val d = FP.env.createInput(hin)//.setParallelism(1)


    val hout = new HadoopOutputFormat(new Hout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    d.writeUsingOutputFormat(hout)

    FP.env.execute

    hout.finalizeGlobal(1)
  }
}


