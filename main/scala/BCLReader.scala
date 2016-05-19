package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP}
import java.nio.{ByteBuffer, ByteOrder}
import java.io.OutputStream
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{GzipCodec, SnappyCodec, Lz4Codec, CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}
import scala.xml.{XML, Node}

import BCL.{Block, ArBlock}

object BCL {
  type Block = Array[Byte]
  type ArBlock = Array[Block]
}

class Fout(filename : String) extends OutputFormat[Block] {
  var writer : OutputStream = null
  def close() = {
    writer.close
  }
  def configure(conf : Configuration) = {
  }
  def open(taskNumber : Int, numTasks : Int) = {
    // val codec = new SnappyCodec //GzipCodec
    val path = new HPath(filename) // + codec.getDefaultExtension)
    val fs = FileSystem.get(new HConf)
    if (fs.exists(path)) 
      fs.delete(path, true)
    val out = fs.create(path)
    /*
    val compressor = new ZlibCompressor(ZlibCompressor.CompressionLevel.BEST_SPEED,
      ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
      ZlibCompressor.CompressionHeader.GZIP_FORMAT,
      64*1024)
      writer = codec.createOutputStream(out) // (out, compressor)
    */
    writer = out
  }
  def writeRecord(rec : Block) = {
    writer.write(rec)
  }
}

object toFQ {
  var header : Block = Array()
  val mid = "\n+\n".getBytes
  val newl = "\n".getBytes
  val dict = CreateTable.getArray
  val toBase : Block = Array('A', 'C', 'G', 'T')
}

class toFQ extends MapFunction[(Block, Int), Block] {
  var blocksize : Int = _
  var bbin : ByteBuffer = _
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
      val o = toFQ.dict(compact(r))
      bbout.putLong(o)
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val idx = b & 0x03
      bbout.put(toFQ.toBase(idx))
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
  def map(x : (Block, Int)) : Block = {
    val b = x._1

    /*
    val pos = x._2
    if (filter(pos))
      return Array()
    */
    blocksize = b.size
    bbin = ByteBuffer.wrap(b)
    val nB = toB
    val nQ = toQ
    // redundant fastq annotation
    b.indices.foreach {i =>
      if (nQ(i) == 0x21.toByte) {
        nQ(i) = 0x23.toByte // #, 0 quality
        nB(i) = 0x4E.toByte // N, no-call
      }
    }
    // val clocs = Locs.readLocs(cpath)
    // toFQ.header ++ h ++ clocs(pos)
    nB ++ toFQ.mid ++ nQ ++ toFQ.newl
  }
}

object readBCL {
  /// block size when reading
  val bsize = 2048
  /// list files from directory
  def getJobs(hp: Array[HPath], splits : Int) : Array[(Array[HPath], Long, Long)] = {
    if (hp.size == 0) throw new Error("No files")
    val fs = FileSystem.get(new HConf)

    val hsize = 4 // size of header
    val totalsize = fs.getFileStatus(hp(0)).getLen - hsize
    val oneblock = ((totalsize / readBCL.bsize) / splits) * readBCL.bsize
    val bar = (Range(0, splits).map(i => i * oneblock).toArray :+ totalsize)
      .map(x => x + 4l)

    Range(0, splits).map(i => (hp, bar(i), bar(i + 1))).toArray
  }
}

class readBCL extends FlatMapFunction[(Array[HPath], Long, Long), (Block, Int)] {
  def openFile(path : HPath) : FSDataInputStream = {
    val fileSystem = FileSystem.get(new HConf)
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
  def readStripe(flist : Array[HPath], start : Long, end : Long, out : Collector[(Block, Int)]) = {
    val streams = flist.map(openFile)
    streams.foreach(_.seek(start))
    var cow = start - 4l
    var buf : ArBlock = null
    while ({buf = aggBlock(streams, end); buf != null}) {
      val pos = Range(0, buf.size).map(cow + _).map(_.toInt)
      pos.indices.foreach(i => out.collect((buf(i), pos(i))))
      cow += pos.size
    }
  }
  def flatMap(work : (Array[HPath], Long, Long), out : Collector[(Block, Int)]) = {
    readStripe(work._1, work._2, work._3, out)
  }
}

object Locs {
  val table = scala.collection.mutable.Map[HPath, Array[Block]]()
  def computeLocs(locsfile : FSDataInputStream) : Array[Block] = {
    def readBin(n : Int) : Array[Block] = {
      val dx = (1000.5 + (n % 82) * 250).toInt
      val dy = (1000.5 + (n / 82) * 250).toInt
      val bs = locsfile.read()
      val r = for (i <- Range(0, bs)) yield {
        val x = locsfile.read()
        val y = locsfile.read()
        (x, y)
      }
      r.toArray.map(a => s"${a._1 + dx}:${a._2 + dy}\n".getBytes)
    }
    locsfile.seek(1)
    val buf = new Array[Byte](4)
    locsfile.read(buf)
    val numbins = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
    Range(0, numbins).toArray.flatMap(n => readBin(n))
  }
  def readLocs(path : HPath) : Array[Block] = {
    if (table.contains(path))
      return table(path)

    val fs = FileSystem.get(new HConf)
    val locsfile = fs.open(path)
    table(path) = computeLocs(locsfile)
    return table(path)
  }
}

object Reader {
  var splits = 1
  /*
      val par = FP.env.getParallelism
      if (par > ret.size)
        splits = scala.math.ceil(par.toDouble / ret.size.toDouble).toInt
   */
  var ranges : Seq[Seq[Int]] = null
  def readFilter(filfile : FSDataInputStream) : Array[Boolean] = {
    filfile.seek(12)
    Iterator.continually{filfile.read()}.takeWhile(_ >= 0).map(_ == 0).toArray
  }
  def readLane(indir : String, lane : Int, outdir : String) : Array[(DataStream[Block], OutputFormat[Block])] = {
    var rr = 1
    val fs = FileSystem.get(new HConf)
    val ldir = s"${indir}L00${lane}/"
    val starttiles = 1101
    val endtiles = 2000
    // process tile
    def process(hp : Array[HPath], tile : Int) : (DataStream[Block], OutputFormat[Block]) = {
      // read filter
      val filter = readFilter(fs.open(new HPath(s"${ldir}/s_${lane}_${tile}.filter")))
      // convert tile -> (x,y)
      val cpath = new HPath(s"${indir}/../L00${lane}/s_${lane}_${tile}.clocs")
      // process splits
      val fout = s"${outdir}L00${lane}/s_${lane}_${tile}-R${rr}.fastq"
      val head = s"${lane}:${tile}:".getBytes
      val work = readBCL.getJobs(hp, splits)
      val in = FP.env.fromCollection(work)//.rebalance
      val d = in
        .flatMap(new readBCL)
        .map(new toFQ) //(head, cpath, filter))
      val hout = new Fout(fout)

      return (d, hout)
    }
    val tiles = Range(starttiles, endtiles)
      .map(x => s"s_${lane}_$x.bcl")
      .map(x => (x, new HPath(s"$ldir/C1.1/$x")))
      .filter(x => fs.isFile(x._2)).map(_._1)
      .toArray

    ranges.flatMap { cycles =>
      val cydirs = cycles
        .map(x => s"$ldir/C$x.1/")
        .map(new HPath(_))
        .filter(fs.isDirectory(_))
        .toArray

      val hp = tiles.map(t => cydirs.map(d => s"$d/$t"))
        .map(t => t.map(s => new HPath(s)))
      if (cycles(0) > 1)
        rr = 2
      val tnum = tiles.map(t => t.substring(4,8).toInt)
      hp.indices.map(i => process(hp(i), tnum(i)))
    }.toArray
  }
  def readAll : Seq[(DataStream[Block], OutputFormat[Block])] = {
    val root = "/home/cesco/dump/data/illumina/"
    val fout = "/home/cesco/dump/data/out/mio/"
    val ldir = "Data/Intensities/BaseCalls/"

    // open runinfo.xml
    val xpath = new HPath(root + "RunInfo.xml")
    val fs = FileSystem.get(new HConf)
    val xin = fs.open(xpath)
    val xsize = fs.getFileStatus(xpath).getLen
    val xbuf = new Array[Byte](xsize.toInt)
    val xml = XML.load(xin)
    // read parameters
    val instrument = (xml \ "Run" \ "Instrument").text
    val runnum = (xml \ "Run" \ "@Number").text
    val flowcell = (xml \ "Run" \ "Flowcell").text
    toFQ.header = s"@$instrument:$runnum:$flowcell:".getBytes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    val lanes = (xml \ "Run" \ "AlignToPhiX" \\ "Lane").map(_.text.toInt)
    // get data from each lane
    val ret = lanes.take(1) // TODO :: remove take(1)
      .flatMap(l => readLane(root + ldir, l, fout))
    ret
  }
}

object test {
  def main(args: Array[String]) {
    val w = Reader.readAll

    w.foreach{ x =>
      (x._1).writeUsingOutputFormat(x._2).setParallelism(1)
    }

    FP.env.execute
  }
}
