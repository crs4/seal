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

import Reader.Block

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

class toFQ extends MapFunction[(Block, Block), Block] {
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
  def map(x : (Block, Block)) : Block = {
    val b = x._1
    val h = x._2

    blocksize = b.size
    bbin = ByteBuffer.wrap(b)
    val nB = toB
    val nQ = toQ
    // apply redundant fastq annotation
    b.indices.foreach {i =>
      if (nQ(i) == 0x21.toByte) {
        nQ(i) = 0x23.toByte // #, 0 quality
        nB(i) = 0x4E.toByte // N, no-call
      }
    }
    h ++ nB ++ toFQ.mid ++ nQ ++ toFQ.newl
  }
}

class readBCL extends FlatMapFunction[(Int, Int), (Block, Block)] {
  def flatMap(input : (Int, Int), out : Collector[(Block, Block)]) = {
    val fs = FileSystem.get(new HConf)
    val (lane, tile) = input
    val h1 = toFQ.header ++ s"${lane}:${tile}:".getBytes
    val h3 = s" 1:N:".getBytes
    // open bcls, filter, control and location files
    val ldir = s"${Reader.root}${Reader.bdir}L00${lane}/"
    val cydirs = Reader.ranges(0)
      .map(x => s"$ldir/C$x.1/")
      .map(new HPath(_))
      .filter(fs.isDirectory(_))
      .toArray
    val flist = cydirs
      .map(d => s"$d/s_${lane}_$tile.bcl")
      .map(s => new HPath(s))

    val bcls = new BCLstream(flist)
    val filter = new Filter(new HPath(s"${Reader.root}${Reader.bdir}L00${lane}/s_${lane}_${tile}.filter"))
    val control = new Control(new HPath(s"${Reader.root}${Reader.bdir}L00${lane}/s_${lane}_${tile}.control"))
    val clocs = new Locs(new HPath(s"${Reader.root}${Reader.bdir}../L00${lane}/s_${lane}_${tile}.clocs"))

    var buf : Array[Block] = null
    while ({buf = bcls.getBlock; buf != null}) {
      buf.foreach{x => 
	val h2 = clocs.getCoord
        val h4 = control.getControl
	if (filter.getFilter == 1.toByte)
	  out.collect(x, h1 ++ h2 ++ h3 ++ h4)}
    }
  }
}

class readBCL2 extends FlatMapFunction[(Int, Int, Int), (Block, Block)] {
  def flatMap(input : (Int, Int, Int), out : Collector[(Block, Block)]) = {
    val fs = FileSystem.get(new HConf)
    val (lane, tile, rr) = input
    val h1 = toFQ.header ++ s"${lane}:${tile}:".getBytes
    val h3 = s" ${rr + 1}:N:".getBytes
    // open bcls, filter, control and location files
    val ldir = s"${Reader.root}${Reader.bdir}L00${lane}/"
    val cydirs = Reader.ranges(rr)
      .map(x => s"$ldir/C$x.1/")
      .map(new HPath(_))
      .filter(fs.isDirectory(_))
      .toArray
    val flist = cydirs
      .map(d => s"$d/s_${lane}_$tile.bcl")
      .map(s => new HPath(s))

    val bcls = new BCLstream(flist)
    val filter = new Filter(new HPath(s"${Reader.root}${Reader.bdir}L00${lane}/s_${lane}_${tile}.filter"))
    val control = new Control(new HPath(s"${Reader.root}${Reader.bdir}L00${lane}/s_${lane}_${tile}.control"))
    val clocs = new Locs(new HPath(s"${Reader.root}${Reader.bdir}../L00${lane}/s_${lane}_${tile}.clocs"))

    var buf : Array[Block] = null
    while ({buf = bcls.getBlock; buf != null}) {
      buf.foreach{x => 
	val h2 = clocs.getCoord
        val h4 = control.getControl
	if (filter.getFilter == 1.toByte)
	  out.collect(x, h1 ++ h2 ++ h3 ++ h4)}
    }
  }
}

class BCLstream(flist : Array[HPath]) {
  val bsize = Reader.bsize
  val fs = FileSystem.get(new HConf)
  val st_end = fs.getFileStatus(flist(0)).getLen
  val streams = flist.map(fs.open)
  streams.foreach(_.seek(4l))
  def readBlock(instream : FSDataInputStream) : Block = {
    val buf = new Block(bsize)
    val r = instream.read(buf)
    if (r > 0)
      return buf.take(r)
    else
      return null
  }
  def getBlock : Array[Block] = {
    if (streams.head.getPos >= st_end)
      return null

    streams.map(readBlock).transpose
  }
}

class Filter(path : HPath) {
  val bsize = Reader.bsize
  val fs = FileSystem.get(new HConf)
  val filfile = fs.open(path)
  filfile.seek(12)
  val buf = new Array[Byte](bsize)
  def readBlock : Iterator[Byte] = {
    val bs = filfile.read(buf)
    buf.toIterator
  }
  var curblock = Iterator[Byte]()
  def getFilter : Byte = {
    if (!curblock.hasNext){
      curblock = readBlock
    }
    curblock.next
  }
}

class Control(path : HPath) {
  val bsize = Reader.bsize << 1
  val fs = FileSystem.get(new HConf)
  val confile = fs.open(path)
  confile.seek(12)
  val buf = new Array[Byte](bsize)
  def readBlock : Iterator[Block] = {
    val bs = confile.read(buf)
    buf.take(bs)
      .sliding(2, 2).map{ x =>
      val con = ByteBuffer.wrap(x).order(ByteOrder.LITTLE_ENDIAN).getShort
      s"$con:0\n".getBytes
    }
  }
  var curblock = Iterator[Block]() 
  def getControl : Block = {
    if (!curblock.hasNext){
      curblock = readBlock
    }
    curblock.next
  }
}

class Locs(path : HPath) {
  val fs = FileSystem.get(new HConf)
  val locsfile = fs.open(path)
  locsfile.seek(1)
  val buf = new Array[Byte](4)
  locsfile.read(buf)
  val numbins = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  def readBin(n : Int) : Iterator[Block] = {
    val dx = (1000.5 + (n % 82) * 250).toInt
    val dy = (1000.5 + (n / 82) * 250).toInt
    val bs = locsfile.read()
    val r = for (i <- Range(0, bs)) yield {
      val x = locsfile.read()
      val y = locsfile.read()
      (x, y)
    }
    r.map(a => s"${a._1 + dx}:${a._2 + dy}".getBytes).toIterator
  }
  var n = 0
  var curblock = Iterator[Block]()
  def getCoord : Block = {
    while(!curblock.hasNext) {
      curblock = readBin(n)
      n += 1
    }
    curblock.next
  }
}

object Reader {
  /// block size when reading
  type Block = Array[Byte]
  val bsize = 2048
  val root = "/home/cesco/dump/data/illumina/"
  val fout = "/home/cesco/dump/data/out/mio/"
  val bdir = "Data/Intensities/BaseCalls/"
  var ranges : Seq[Seq[Int]] = null
  def procSingleRead(input : (Int, Int)) : (DataStream[Block], OutputFormat[Block]) = {
    val (lane, tile) = input
    val in = FP.env.fromElements(input)
    val bcl = in.flatMap(new readBCL)
    val oname = s"${fout}s_${lane}_${tile}.fastq"
    val hout = new Fout(oname)
    val dsout = bcl.map(new toFQ)
    (dsout, hout)
  }
  def procDoubleRead(input : (Int, Int)) : Seq[(DataStream[Block], OutputFormat[Block])] = {
    val (lane, tile) = input
    ranges.indices.map{ rr =>
      val in = FP.env.fromElements((lane, tile, rr))
      val bcl = in.flatMap(new readBCL2)
      val oname = s"${fout}s_${lane}_${tile}-R${rr + 1}.fastq"
      val hout = new Fout(oname)
      val dsout = bcl.map(new toFQ)
      (dsout, hout)
    }
  }
  // process tile
  def process(input : (Int, Int)) = {
    val stuff = ranges.size match {
      case 1 => Seq(procSingleRead(input))
      case 2 => procDoubleRead(input)
    }
    stuff.foreach(x => x._1.writeUsingOutputFormat(x._2).setParallelism(1))
  }
  def readLane(lane : Int, outdir : String) : Seq[(Int, Int)] = {
    val fs = FileSystem.get(new HConf)
    val ldir = s"${root}${bdir}L00${lane}/"
    val starttiles = 1101
    val endtiles = 2000

    val tiles = Range(starttiles, endtiles)
      .map(x => s"s_${lane}_$x.bcl")
      .map(x => (x, new HPath(s"$ldir/C1.1/$x")))
      .filter(x => fs.isFile(x._2)).map(_._1)
      .toArray

    val tnum = tiles.map(t => t.substring(4,8).toInt)
    tnum.map((lane, _))
  }

  def getAllJobs : Seq[(Int, Int)] = {
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
    // reads and indexes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    val lanes = (xml \ "Run" \ "AlignToPhiX" \\ "Lane").map(_.text.toInt)
    // get data from each lane
    lanes//.take(1) // TODO :: remove take(1)
      .flatMap(l => readLane(l, fout))
  }
}

object test {
  def main(args: Array[String]) {
    val w = Reader.getAllJobs

    w.foreach(Reader.process)
    FP.env.execute
  }
}
