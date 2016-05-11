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
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}

import BCL.{Block, ArBlock}

object BCL {
  type Block = Array[Byte]
  type ArBlock = Array[Block]
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

class toFQ extends MapFunction[Block, Block] {
  val toBase : Block = Array('A', 'C', 'G', 'T')
  var blocksize : Int = _
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
    val bbout = ByteBuffer.allocate(blocksize)
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
    val bbout = ByteBuffer.allocate(blocksize)
    while(bbin.remaining > 7) {
      val r = bbin.getLong
      bbout.putLong(0x2121212121212121l + ((r & 0xFCFCFCFCFCFCFCFCl) >>> 2))
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val q = (b & 0xFF) >>> 2
      bbout.put((0x21 + q).toByte)
    }
    bbout.array
  }
  val newl = 0x0a.toByte
  def map(b : Block) : Block = {
    blocksize = b.size
    bbin = ByteBuffer.wrap(b)
    (toB :+ newl) ++ toQ ++ Array(newl, newl)
  }
}

object readBCL {
  /// block size when reading
  val bsize = 2048
  /// list files from directory
  def getJobs(hp: Array[HPath], splits : Int) : Array[(Array[HPath], (Long, Long))] = {
    if (hp.size == 0) throw new Error("No files")
    val conf = new HConf
    val fs = FileSystem.get(conf)

    val hsize = 4 // size of header
    val totalsize = fs.getFileStatus(hp(0)).getLen - hsize
    val oneblock = ((totalsize / bsize) / splits) * bsize
    val bar = (Range(0, splits).map(i => i * oneblock).toArray :+ totalsize)
      .map(x => x + 4l)

    Range(0, splits).map(i => (hp, (bar(i), bar(i + 1)))).toArray
  }
}

class readBCL extends FlatMapFunction[(Array[HPath], (Long, Long)), Block] {
  def openFile(path : HPath) : FSDataInputStream = {
    val conf = new HConf
    val fileSystem = FileSystem.get(conf)
    fileSystem.open(path)
  }
  def readBlock(instream : FSDataInputStream) : Block = {
    val buf = new Block(readBCL.bsize)
    val r = instream.read(buf)
    if (r > 0)
      return buf.take(r)
    else
      return null
  }
  def aggBlock(in : Array[FSDataInputStream], end : Long) : ArBlock = {
    if (in.head.getPos >= end)
      return null

    in.map(readBlock).transpose
  }
  def readStripe(flist : Array[HPath], start : Long, end : Long, out : Collector[Block]) = {
    val streams = flist.map(openFile)
    streams.foreach(_.seek(start))
    var buf : ArBlock = null
    while ({buf = aggBlock(streams, end); buf != null}) {
      buf.foreach(out.collect)
    }
  }
  def flatMap(work : (Array[HPath], (Long, Long)), out : Collector[Block]) = {
    readStripe(work._1, work._2._1, work._2._2, out)
  }
}

object Read {
  def process(hp : Array[HPath], fout : String) : (DataStream[(Void, Block)], HadoopOutputFormat[Void, Block]) = {
    def void: Void = null
    val splits = 1
    val work = readBCL.getJobs(hp, splits)
    val in = FP.env.fromCollection(work)//.rebalance
    val d = in
      .flatMap(new readBCL)
      .map(new toFQ)
      .map(x => (void, x))
    
    val job = Job.getInstance(FP.conf)
    val hout = new HadoopOutputFormat(new BHout, job)
    // val hout = new HadoopOutputFormat(new NullOutputFormat[Void, Block], job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    return (d, hout)
  }
  def readLane(indir : String, lane : Int, outdir : String) : Array[(DataStream[(Void, Block)], HadoopOutputFormat[Void, Block])] = {
    val ldir = s"${indir}L00${lane}/"
    val maxcycles = 1000
    val starttiles = 1101
    val endtiles = 2000
    val conf = new HConf
    val fs = FileSystem.get(conf)
    val cydirs = Range(1, maxcycles)
      .map(x => s"$ldir/C$x.1/")
      .map(new HPath(_))
      .filter(fs.isDirectory(_))
      .toArray
    val tiles = Range(starttiles, endtiles)
      .map(x => s"s_${lane}_$x.bcl")
      .map(x => (x, new HPath(s"$ldir/C1.1/$x")))
      .filter(x => fs.isFile(x._2)).map(_._1)
      .toArray

    val hp = tiles.map(t => cydirs.map(d => s"$d/$t"))
      .map(t => t.map(s => new HPath(s)))

    hp.indices.map(i => process(hp(i), s"${outdir}L00${lane}/${tiles(i)}.fastq")).toArray
  }
  // test
  def main(args: Array[String]) {
    val root = "/home/cesco/dump/data/Intensities/"
    val fout = "/home/cesco/dump/data/out/"

    // FP.env.setParallelism(4)

    val ldir = "BaseCalls/"
    val lanenum = 8
    val w = Range(1, lanenum + 1).flatMap(l => readLane(root + ldir, l, fout))

    // to write files in more pieces enable rebalance and comment setParallelism(1)
    w.foreach{ x =>
      (x._1)//.rebalance
        .writeUsingOutputFormat(x._2).setParallelism(1)
    }

    FP.env.execute
    w.foreach(x => (x._2).finalizeGlobal(1))
  }
}
