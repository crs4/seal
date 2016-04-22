package bclconverter.bclreader

import bclconverter.{FlinkProvider => FP}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.api.scala._
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
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

class readBCL extends MapFunction[String, ArBlock] {
  def map(filename : String) : ArBlock = {
    val path = new HPath(filename)
    val conf = new HConf
    val fileSystem = FileSystem.get(conf)
    val instream = fileSystem.open(path)
    val bsize = 2048
    var buf = new Block(bsize)
    var ret : ArBlock = Array()
    var r = 0
    while ({r = instream.read(buf); r == bsize}) {
      ret :+= buf
    }
    // last block might have length r s.t. 0 < r < bsize
    if (r > 0) {
      buf = buf.take(r)
      ret :+= buf
    }
    ret
  }
}

class Aggr extends ReduceFunction[Array[ArBlock]] {
  def reduce(left : Array[ArBlock], right : Array[ArBlock]) : Array[ArBlock] = {
    left.indices.toArray.map(i => left(i) ++ right(i))
  }
}

class toFQ extends MapFunction[ArBlock, String] {
  def intmap(b : Block) : String = {
    new String(b.map(BCL.toB)) + "\n" + new String(b.map(BCL.toQ))
  }
  def map(ab : ArBlock) : String = {
    ab.transpose.map(intmap).mkString("", "\n\n", "\n\n")
  }
}

object Read {
  val root = "/home/cesco/dump/data/t/"
  val fin = root + "boh"
  val fout = root + "out"
  val ciaos = Range(1101, 1201).map(x => root + s"ciao/ciao_$x.bin")

  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(1)

    val in = FP.env.fromCollection(ciaos).rebalance
    val d = in // DS[String]
      .map(new readBCL) // DS[ArBlock]
      .map(x => x.map(b => Array(b))) // DS[Array[ArBlock]]
      .reduce(new Aggr) // DS[Array[ArBlock]]
      .flatMap(x => x) // DS[Array[ArBlock]]
      .map(new toFQ) // DS[String]
      .map(x => (void, x)) //DS[Void, String]
    
    val job = Job.getInstance(FP.conf)
    val hout = new HadoopOutputFormat(new BHout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)
    d.output(hout).setParallelism(1) //d.writeUsingOutputFormat(hout)

    FP.env.execute

    // hout.finalizeGlobal(1)
  }
}


