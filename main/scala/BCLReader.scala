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
import Table.toQuartet

object BCL {
  type Block = Array[Byte]
  type ArBlock = Array[Block]
  type DS = DataStream[(Void, String)] // DataStream or DataSet
}

class BHout extends HFileOutputFormat[Void, String] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, String] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: String) = {
      fileOut.write(what.getBytes)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, String] = {
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
    var buf = new Block(bsize)
    val r = instream.read(buf)
    if (r > 0)
      buf = buf.take(r)
    else
      buf = null
    buf
  }
  def aggBlock(in : Array[FSDataInputStream]) : ArBlock = {
    val r = in.map(readBlock)
    if (r.head == null)
      return null
    else
      return r.transpose
  }
  def flatMap(flist : Array[String], out : Collector[Block]) = {
    val incyc = flist.map(openFile)
    var buf : ArBlock = null
    while ({buf = aggBlock(incyc); buf != null}) {
      buf.foreach(out.collect)
    }
  }
}


class toFQ2 extends MapFunction[Block, String] {
  val toBAr : Block = Array('A', 'C', 'G', 'T')
  def toB(b : Byte) : Byte = {
    val idx = b & 0x03
    toBAr(idx)
  }
  def toQ(b : Byte) : Byte = {
    val q = (b & 0xFF) >>> 2
    (0x40 + q).toByte
  }
  def map(b : Block) : String = {
    val sB = b.map(toB)
    val sQ = b.map(toQ)
    new String(sB, "US-ASCII") + "\n" + new String(sQ, "US-ASCII") + "\n\n"
  }
}

class toFQ extends MapFunction[Block, String] {
  val toBase : Block = Array('A', 'C', 'G', 'T')
  var rsize : Int = _
  var bsize : Int = _
  var bbin : ByteBuffer = _
  var dict = CreateTable.getArray
  def compact2(l : Long) : Int = {
    val i1 = (l >>> 30).toInt
    val i2 = (l + i1).toInt
    val i3 = i2 & 0xFFFF
    val i4 = i2 >>> 12
    i3 + i4
  }
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
  def toB2 : Block = {
    bbin.rewind
    val bbout = ByteBuffer.allocate(bsize)
    while(bbin.remaining > 3) {
      val r = bbin.getInt & 0x03030303
      val o = toQuartet(r)
      bbout.putInt(o)
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
  def map(b : Block) : String = {
    bsize = b.size
    rsize = b.size >>> 3
    bbin = ByteBuffer.wrap(b)
    val sQ = toQ
    val sB = toB
    new String(sB, "US-ASCII") + "\n" + new String(sQ, "US-ASCII") + "\n\n"
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

  def process(dir : String, fout : String) : (DS, HadoopOutputFormat[Void, String]) = {
    def void: Void = null
    val ciaos = makeList(dir)
    val in = FP.env.fromElements(ciaos)
    val d = in // DS[Array[String]]
      .flatMap(new readBCL) // DS[ArBlock]
      .map(new toFQ) // DS[String]
      .map(x => (void, x)) //DS[Void, String]
    
    val job = Job.getInstance(FP.conf)
    val hout = new HadoopOutputFormat(new BHout, job)
    // val hout = new HadoopOutputFormat(new NullOutputFormat[Void, String], job)
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
      // (x._1).output(x._2).setParallelism(1) // DataSet
      (x._1).writeUsingOutputFormat(x._2).setParallelism(1) // DataStream
    }

    FP.env.execute
    w.foreach(x => (x._2).finalizeGlobal(1)) // DataStream
  }
}


