package bclconverter.bclreader

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
class readBCL extends FlatMapFunction[(String, Int), (Int, Int, Int, Block)] {
  def flatMap(filecycle : (String, Int), out : Collector[(Int, Int, Int, Block)]) = {
    val tile = 0 // TODO: to be paramatrized later
    val cycle = filecycle._2 //
    val path = new HPath(filecycle._1)
    val conf = new HConf
    val fileSystem = FileSystem.get(conf)
    val instream = fileSystem.open(path)
    val bsize = 2048
    var buf = new Block(bsize)
    var bnum = 0
    var r = 0
    while ({r = instream.read(buf); r == bsize}) {
      out.collect((tile, cycle, bnum, buf))
      bnum += 1
    }
    // last block might have length r s.t. 0 < r < bsize
    if (r > 0) {
      buf = buf.take(r)
      out.collect((tile, cycle, bnum, buf))
    }
  }
}

object Aggr {
  def reduce(all : Iterator[(Int, Int, Int, Block)]) : (Int, Int, ArBlock) = {
    val arr = all.toArray
    val r = arr.map(_._4).transpose // It[Block]      
    
    val h = arr.head
    (h._1, h._3, r)
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
  def makeList(dirIN : String) : Array[(String, Int)] = {
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
  }

  def process(dir : String, fout : String) {
    def void: Void = null
    val ciaos = makeList(dir)
    val in = FP.env.fromCollection(ciaos).rebalance
    val d = in // DS[(String, Int)]
      .flatMap(new readBCL) // DS[(Int, Int, Int, Block)]
      .groupBy(0, 2).sortGroup(1, Order.ASCENDING)
      .reduceGroup(Aggr.reduce(_)) // DS[(Int, Int, ArBlock)]
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

    Range(1, 5).map("_" + _).foreach(x => process(root + dirname + x, fout + x))

    FP.env.execute

    // hout.finalizeGlobal(1)
  }
}


