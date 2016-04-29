package bclconverter.bclreader2

import bclconverter.{FlinkProvider => FP}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}

import BCL.{Block, ArBlock}

object BCL {
  type Block = Array[Byte]
  type ArBlock = Array[Block]
  val toBAr : Block = Array('A', 'C', 'G', 'T')
  def toB(b : Byte) : Byte = {
    val idx = b & 0x03
    toBAr(idx.toByte)
  }
  def toQ(b : Byte) : Byte = {
    (0x40 + ((b & 0xFC) >> 2)).toByte
  }
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

// filaname -> (Tile, Cycle, bnum, block)
class readBCL extends FlatMapFunction[Array[String], (Int, Int, ArBlock)] {
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
    val r = in.map(readBlock(_))
    if (r.head == null)
      return null
    else
      return r.transpose
  }
  def flatMap(flist : Array[String], out : Collector[(Int, Int, ArBlock)]) = {
    val tile = 0 // TODO: to be paramatrized later
    val incyc = flist.map(openFile)
    var bnum = 0
    var buf : ArBlock = null
    while ({buf = aggBlock(incyc); buf != null}) {
      out.collect((tile, bnum, buf))
      bnum += 1
    }
  }
}

/*
class Aggr extends GroupReduceFunction[(Int, Int, Int, Block), (Int, Int, ArBlock)] {
  def reduce(all : Iterable[(Int, Int, Int, Block)], out : Collector[(Int, Int, ArBlock)]) : Unit = {
    val arr = all.toArray
    val r = arr.map(_._4).transpose // It[Block]      
    
    val h = arr.head
    out.collect((h._1, h._3, r))
  }
}
*/

class toFQ extends MapFunction[(Int, Int, ArBlock), (Int, Int, String)]
  with FlatMapFunction[(Int, Int, ArBlock), (Int, Int, String)] {
  def intmap(b : Block) : String = {
    new String(b.map(BCL.toB)) + "\n" + new String(b.map(BCL.toQ)) + "\n"
  }
  def map(in : (Int, Int, ArBlock)) : (Int, Int, String) = {
    val ab = in._3
    val r = ab.map(intmap).mkString("", "\n", "\n")
    (in._1, in._2, r)
  }
  def flatMap(in : (Int, Int, ArBlock), out : Collector[(Int, Int, String)]) = {
    val ab = in._3
    val r = ab.map(intmap)
      .map(x => (in._1, in._2, x))
      .foreach(out.collect(_))
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

  def process(dir : String, fout : String) {
    def void: Void = null
    val ciaos = makeList(dir)
    val in = FP.env.fromElements(ciaos)
    val d = in // DS[Array[String]]
      .flatMap(new readBCL) // DS[(Int, Int, ArBlock)]
      .map(new toFQ).sortPartition(1, Order.ASCENDING) // DS[(Int, Int, String)]
      .map(x => (void, x._3)) //DS[Void, String]
    
    val job = Job.getInstance(FP.conf)
    val hout = new HadoopOutputFormat(new BHout, job)
    // val hout = new HadoopOutputFormat(new NullOutputFormat[Void, String], job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)
    d.output(hout).setParallelism(1) //d.writeUsingOutputFormat(hout)
  }

  // test
  def main(args: Array[String]) {
    val root = "/home/cesco/dump/data/t/"

    val fout = root + "out"
    val dirname = "ciao"

    FP.env.setParallelism(4)

    Range(1, 4).map("_" + _).foreach(x => process(root + dirname + x, fout + x))

    FP.env.execute

    // hout.finalizeGlobal(1)
  }
}


