package bclconverter.blockhadoop

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
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingTimeWindows, SlidingTimeWindows, SlidingEventTimeWindows}
import scala.util.Random
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger, EventTimeTrigger}
import org.apache.flink.api.common.functions.MapFunction


object BCL {
  def toBQ(b : Byte) : (Byte, Byte) = {
    return((b & 0x03).toByte, ((b & 0xFC) >> 2).toByte)
  }
}


case class BCL(b : Byte) {
}

class Hout2 extends HFileOutputFormat[Void, Byte] {
  val bsize = 64*1024
  var buf = new Array[Byte](bsize)
  var lpos = 0 // local pos
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Byte] {
    println("-------> new record writer created")

    def close(ta : TaskAttemptContext) = {
      fileOut.write(buf, 0, lpos)
      fileOut.close
    }
    def write(void: Void, what: Byte) = {
      buf(lpos) = what
      lpos += 1
      if (lpos == bsize) {
        lpos = 0        
        for (b <- buf.grouped(256)){
          fileOut.write(b)
        }
      }
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, Byte] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}



class Hout extends HFileOutputFormat[Void, Byte] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Byte] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: Byte) = {
      fileOut.writeByte(what)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, Byte] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}


class BHout extends HFileOutputFormat[Void, Array[Byte]] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Array[Byte]] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: Array[Byte]) = {
      fileOut.write(what)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, Array[Byte]] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}


class BHin extends HFileInputFormat[Void, Array[Byte]] {
  class RR extends RecordReader[Void, Array[Byte]] {
    def void: Void = null
    val bsize = 1024*1024
    var cvalue = new Array[Byte](bsize)
    var start = -1l
    var pos = -1l
    var end = -1l
    var fileIn : FSDataInputStream = null

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

    def getCurrentValue : Array[Byte] = {
      cvalue
    }

    def nextKeyValue : Boolean = {
      if (pos != end) {
        val r = fileIn.read(cvalue)
        pos += r
        return (r > 0)
      }
      else
        false
    }
  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[Void, Array[Byte]] = {
    new RR
  }
}


class Counter extends SourceFunction[Byte] {
  var go = true
  var count = 0
  var num : Byte = 0
  val rep = 128*1024*1024

  def cancel = {
    go = false
  }

  def run(ctx : SourceContext[Byte]){
    while (go && count < rep) {
      ctx.collect(num)
      count += 1
      num = (num + 1).toByte
    }
  }
}

class BCounter extends SourceFunction[Array[Byte]] {
  var go = true
  var count = 0
  val bsize = 1024
  val rep = 128*1024*1024/bsize
  
  val buf = new Array[Byte](bsize)
  (new Random).nextBytes(buf)

  def cancel = {
    go = false
  }

  def run(ctx : SourceContext[Array[Byte]]){
    while (go && count < rep) {
      ctx.collect(buf)
      count += 1
    }
  }
}

class BBCounter extends SourceFunction[Array[BCL]] {
  var go = true
  var count = 0
  val bsize = 1024
  val rep = 128*1024*1024/bsize
  
  val buf = new Array[Byte](bsize)
  (new Random).nextBytes(buf)

  val bbuf = buf.map(x => BCL(x))

  def cancel = {
    go = false
  }

  def run(ctx : SourceContext[Array[BCL]]){
    while (go && count < rep) {
      ctx.collect(bbuf)
      count += 1
    }
  }
}

object Aggr {
  val coso = new Array[Byte](256*32)
}

class Aggr[W <: Window] extends AllWindowFunction[Byte, Array[Byte], W] {
  def apply(win : W, in : Iterable[Byte], out : Collector[Array[Byte]]) = {
    out.collect(in.toArray) ///// ---> in.toArray
  }
}

class Aggr2[W <: Window] extends AllWindowFunction[Array[Byte], Array[Byte], W] {
  def apply(win : W, in : Iterable[Array[Byte]], out : Collector[Array[Byte]]) = {
    for (b <- in){
      out.collect(b)
    }
  }
}

class BAggr2[W <: Window] extends AllWindowFunction[Array[BCL], Array[Byte], W] {
  def apply(win : W, in : Iterable[Array[BCL]], out : Collector[Array[Byte]]) = {
    for (x <- in){
      val a = x.map(_.b).toArray
      out.collect(a)
    }
  }
}


class BAggr[W <: Window] extends AllWindowFunction[BCL, Array[Byte], W] {
  def apply(win : W, in : Iterable[BCL], out : Collector[Array[Byte]]) = {
    val a = in.map(_.b).toArray
    out.collect(a)
  }
}

object Filtro {
  val random = new Random
}

class Filtro extends MapFunction[Array[Byte], Array[Byte]]{
  def map(in : Array[Byte]) : Array[Byte] = {
    in.map(b => (b & 0x12).toByte)
  }
}

class Filtro2 extends MapFunction[Byte, Byte]{
  def map(b : Byte) : Byte = {
    (b & 0x12).toByte
  }
}


object HReader {
  val fin = "/tmp/t/huge"
  val fout = "/tmp/t/out"

  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(2)

    val job = Job.getInstance(FP.conf)

    val hin = new HadoopInputFormat(new BHin, classOf[Void], classOf[Array[Byte]], job)
    val hipath = new HPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1024*1024)
    val d = FP.env.createInput(hin) // DataStream[(Void, Array[Byte])]

    // val cow = FP.env.addSource(new Counter)


    val hout = new HadoopOutputFormat(new BHout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    val bsize = 256

    val sin = d.map(x => x._2)
      .map(new Filtro)
    val sout = sin.map(x => (void, x))

    /*
    val sout = cow
      // .map(b => BCL(b))
      .countWindowAll(bsize).apply(new Aggr[GlobalWindow])
      .map(x => (void, x))
    */

    sout.writeUsingOutputFormat(hout)

    FP.env.execute

    hout.finalizeGlobal(1)
  }
}


