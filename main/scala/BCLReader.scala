package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP}
import java.math.BigInteger
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
    val r = in.map(readBlock(_))
    if (r.head == null)
      return null
    else
      return r.transpose
  }
  def flatMap(flist : Array[String], out : Collector[Block]) = {
    val incyc = flist.map(openFile)
    var buf : ArBlock = null
    while ({buf = aggBlock(incyc); buf != null}) {
      buf.foreach(out.collect(_))
    }
  }
}

class toFQ extends MapFunction[Block, String] {
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

/*
class toFQ2(numcycles : Int) extends MapFunction[Block, String] {
  def toB(b : Byte) : Byte = {
    val idx = b & 0x03
    toBAr(idx)
  }
  val toBAr : Block = Array('A', 'C', 'G', 'T')
  val c03 = new BigInteger(Array.fill[Byte](numcycles)(0x03))
  val cFC = new BigInteger(Array.fill[Byte](numcycles)(0xFC.toByte))
  val c40 = new BigInteger(Array.fill[Byte](numcycles)(0x40))

  def map(b : Block) : String = {
    val big = new BigInteger(b)
    val qual = big.and(cFC).shiftRight(2).add(c40)
    val seq = big.and(c03).toByteArray.map(toBAr(_))
    
    new String(seq) + "\n" + new String(qual.toByteArray) + "\n"
  }
}
 */

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

    val w = Range(1, 3).map("_" + _).map(x => process(root + dirname + x, fout + x))

    w.foreach{ x =>
      // (x._1).output(x._2).setParallelism(1) // DataSet
      (x._1).writeUsingOutputFormat(x._2).setParallelism(1) // DataStream
    }

    FP.env.execute
    w.foreach(x => (x._2).finalizeGlobal(1)) // DataStream
  }
}


