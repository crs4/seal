package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP}
import java.nio.ByteBuffer
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}

import BCL.{Block, ArBlock, DS}

object BCL {
  type Block = Array[Byte]
  type ArBlock = Array[Block]
  type DS = DataStream[(Void, Block)] // DataStream or DataSet
}

class BHout extends HFileOutputFormat[Void, Block] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Block] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: Block) = {
      fileOut.write(what)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, Block] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)
    
    new RW
  }
}

class readBCL extends FlatMapFunction[Array[String], Block] {
  val bsize = 2048
  def openFile(filename : String) : FSDataInputStream = {
    val path = new HPath(filename)
    val conf = new HConf
    val fileSystem = FileSystem.get(conf)
    fileSystem.open(path)
  }
  def readBlock(instream : FSDataInputStream) : Block = {
    val buf = new Block(bsize)
    val r = instream.read(buf)
    if (r > 0)
      return buf.take(r)
    else
      return null
  }
  def aggBlock(in : Array[FSDataInputStream]) : ArBlock = {
    val r = in.map(readBlock)
    if (r.head == null)
      return null
    else
      return r.transpose
  }
  def flatMap(flist : Array[String], out : Collector[Block]) = {
    val streams = flist.map(openFile)
    var buf : ArBlock = null
    while ({buf = aggBlock(streams); buf != null}) {
      buf.foreach(out.collect)
    }
  }
}

class toFQ extends MapFunction[Block, Block] {
  val toBase : Block = Array('A', 'C', 'G', 'T')
  var bsize : Int = _
  var bbin : ByteBuffer = _
  val dict = CreateTable.getArray
  def compact(l : Long) : Int = {
    val i0 = (l & 0x000000000000000Fl)
    val i1 = (l & 0x0000000000000F00l) >>>  6
    val i2 = (l & 0x00000000000F0000l) >>> 12
    val i3 = (l & 0x000000000F000000l) >>> 18
    val i4 = (l & 0x0000000F00000000l) >>> 24
    val i5 = (l & 0x00000F0000000000l) >>> 30
    val i6 = (l & 0x000F000000000000l) >>> 36
    val i7 = (l & 0x0F00000000000000l) >>> 42
    (i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7).toInt 
  }
  def toB : Block = {
    bbin.rewind
    val bbout = ByteBuffer.allocate(bsize)
    while(bbin.remaining > 7) {
      val r = bbin.getLong & 0x0303030303030303l
      val o = dict(compact(r))
      bbout.putLong(o)
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val idx = b & 0x03
      bbout.put(toBase(idx))
    }
    bbout.array
  }
  def toQ : Block = {
    bbin.rewind    
    val bbout = ByteBuffer.allocate(bsize)
    while(bbin.remaining > 7) {
      val r = bbin.getLong
      bbout.putLong(0x4040404040404040l + ((r & 0xFCFCFCFCFCFCFCFCl) >>> 2))
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val q = (b & 0xFF) >>> 2
      bbout.put((0x40 + q).toByte)
    }
    bbout.array
  }
  val newl = 0x0a.toByte
  def map(b : Block) : Block = {
    bsize = b.size
    bbin = ByteBuffer.wrap(b)
    (toB :+ newl) ++ toQ ++ Array(newl, newl)
  }
}


object Read {
  // list files from directory
  def makeList(dirIN : String) : Array[String] = {
    val path = new HPath(dirIN)
    val conf = new HConf
    val fs = FileSystem.get(conf)
    val files = fs.listFiles(path, false)
    if (!files.hasNext) throw new Error("No files")
    var r : Array[String] = Array()
    while (files.hasNext) {
      r :+= files.next.getPath.toString
    }
    r.map(x => (x, x.substring(43,47).toInt))
      .sortBy(_._2).map(_._1)
  }

  def process(dir : String, fout : String) : (DS, HadoopOutputFormat[Void, Block]) = {
    def void: Void = null
    val ciaos = makeList(dir)
    val in = FP.env.fromElements(ciaos)
    val d = in // DS[Array[String]]
      .flatMap(new readBCL) // DS[Block]
      .map(new toFQ) // DS[Block]
      .map(x => (void, x)) //DS[Void, Block]
    
    val job = Job.getInstance(FP.conf)
    val hout = new HadoopOutputFormat(new BHout, job)
    // val hout = new HadoopOutputFormat(new NullOutputFormat[Void, Block], job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    return (d, hout)
  }

  // test
  def main(args: Array[String]) {
    val root = "/home/cesco/dump/data/t/"

    val fout = root + "out"
    val dirname = "ciao"

    FP.env.setParallelism(1)

    val w = Range(1, 5).map("_" + _).map(x => process(root + dirname + x, fout + x))

    w.foreach{ x =>
      // (x._1).output(x._2).setParallelism(1) // DataSet only
      (x._1).writeUsingOutputFormat(x._2).setParallelism(1) // DataStream only
    }

    FP.env.execute
    w.foreach(x => (x._2).finalizeGlobal(1)) // DataStream only
  }
}
