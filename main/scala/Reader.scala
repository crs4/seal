package bclconverter.reader

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
import org.apache.hadoop.io.{LongWritable, ByteWritable, IntWritable}

object BCL {
  type Robo = LongWritable
  val rsize = 8
  def toBQ(b : Byte) : (Byte, Byte) = {
    return((b & 0x03).toByte, ((b & 0xFC) >> 2).toByte)
  }
  def unwrap(l : Long) : Array[Byte] = {
    val mini = ByteBuffer.allocate(rsize)
    mini.putLong(l)
    mini.array
  }
  def wrapSeq(b : Iterator[Byte]) : Iterator[Long] = {
    b.grouped(8).map(x => ByteBuffer.wrap(x.toArray).getLong)
  }
}

import BCL.{Robo, rsize}

case class BCL(b : Byte) {
}


class Hout extends HFileOutputFormat[Void, Robo] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Robo] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: Robo) = {
      fileOut.writeLong(what.get)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter [Void, Robo] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}

class Hin extends HFileInputFormat[Void, Robo] {
  class RR extends RecordReader[Void, Robo] {
    def void: Void = null
    var start = -1l
    var pos = -1l
    var end = -1l
    var fileIn : FSDataInputStream = null
    var cvalue : Robo = _

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

    def getCurrentValue : Robo = {
      cvalue
    }

    def nextKeyValue : Boolean = {
      if (pos != end) {
        cvalue = new Robo(fileIn.readLong)
        pos += 8
        true
      }
      else
        false
    }

  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[Void, Robo] = {
    new RR
  }
}



object HReader {
  val fin = "/tmp/t/big"
  val fout = "/tmp/t/out"
  
  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(1)

    // val dd = FP.env.createInput(new FBQin(fin)).map(x => (void, x))

    val job = Job.getInstance(FP.conf)

    val hin = new HadoopInputFormat(new Hin, classOf[Void], classOf[Robo], job)
    val hipath = new HPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1024*1024)
    val d = FP.env.createInput(hin)
      // .flatMap(x => BCL.unwrap(x._2.get))


    val hout = new HadoopOutputFormat(new Hout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)


    d
      //.filter(_._1 != void )
    /*
      .keyBy(0).fold(Nil : Seq[Long])((i : Byte, par) => {
      i += 1
      if (i == 8)
        return 123
      else
        return Nil
       })
     */
      .writeUsingOutputFormat(hout)

    FP.env.execute

    hout.finalizeGlobal(1)
  }
}


