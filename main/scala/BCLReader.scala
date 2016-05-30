package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP}
import java.io.OutputStream
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import scala.io.Source
import scala.xml.{XML, Node}
// import org.apache.hadoop.io.compress.zlib.ZlibCompressor
// import org.apache.hadoop.io.compress.{GzipCodec, SnappyCodec, Lz4Codec, CompressionCodec, CompressionCodecFactory}

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

object Reader {
  /// block size when reading
  type Block = Array[Byte]
  val bsize = 2048
  val mismatches = 1
  val root = "/home/cesco/dump/data/illumina/"
  val fout = "/home/cesco/dump/data/out/mio/"
  val bdir = "Data/Intensities/BaseCalls/"
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var sampleMap = Map[(Int, String), String]()
  val undet = "Undetermined"
  var fuz : fuzzyIndex = null
  def procReads(input : (Int, Int)) : Seq[(DataStream[Block], OutputFormat[Block])] = {
    val (lane, tile) = input
    val in = FP.env.fromElements(input)
    val bcl = in.flatMap(new readBCL).split{
      input : (Block, Int, Block, Block) =>
	(input._2) match {
          case 0 => List("R1")
          case 1 => List("R2")
	}
    }
    val rreads = Array("R1", "R2")
    var houts = rreads.map{ rr =>
      sampleMap.filterKeys(_._1 == lane)
      .map {
	case (k, pref) => ((k._1, k._2) -> new Fout(f"${fout}${pref}_L${k._1}%03d_${tile}-${rr}.fastq"))
      }
    }
    rreads.indices.foreach{
      i => (houts(i) += (lane, undet) -> new Fout(f"${fout}Undetermined_L${lane}%03d_${tile}-${rreads(i)}.fastq"))
    }
    val stuff = rreads.indices.map{ i =>
      bcl.select(rreads(i))
      .map(x => (x._1, x._3, x._4))
      .split{
	input : (Block, Block, Block) =>
	  new String(input._1) match {
            case x => List(fuz.getIndex((lane, x)))
	  }
      }
    }
    val output = rreads.indices.flatMap{ i =>
      houts(i).keys.map{ k =>
	val ds = stuff(i).select(k._2).map(x => (x._2, x._3)).map(new toFQ)
	val ho = houts(i)(k)
	(ds, ho)
      }
    }
    return output
  }
  // process tile
  def process(input : (Int, Int)) = {
    val stuff = procReads(input)
    stuff.foreach(x => x._1.writeUsingOutputFormat(x._2).setParallelism(1))
  }
  def readLane(lane : Int, outdir : String) : Seq[(Int, Int)] = {
    val fs = FileSystem.get(new HConf)
    val ldir = f"${root}${bdir}L${lane}%03d/"
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
  def readSampleNames = { //: Map[(Int, Block), String)] = {
    // open runinfo.xml
    val path = new HPath(root + "SampleSheet.csv")
    val fs = FileSystem.get(new HConf)
    val in = fs.open(path)
    val fsize = fs.getFileStatus(path).getLen
    val coso = Source.createBufferedSource(in).getLines.map(_.trim).toArray
    val tags = coso.indices.map(i => (i, coso(i))).filter(x => x._2.startsWith("[")) :+ (coso.size, "[END]")
    val drange = tags.indices.dropRight(1)
      .map(i => ((tags(i)._1, tags(i + 1)._1), tags(i)._2))
      .find(_._2 == "[Data]")
    val dr = drange match {
      case Some(x) => x._1
      case None => throw new Error("No [Data] section in SampleSheet.csv")
    }
    val csv = coso.slice(dr._1, dr._2).drop(2)
      .map(_.split(","))
    csv.foreach(l => sampleMap += (l(1).toInt, l(4)) -> l(2))
    fuz = new fuzzyIndex(sampleMap)
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
    header = s"@$instrument:$runnum:$flowcell:".getBytes
    // reads and indexes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    index = reads.indices.map(i => (fr(i), fr(i + 1) - 1, reads(i)._2)) // for some reason the last base of the index is ignored...
      .filter(_._3 == "Y").map(x => Range(x._1, x._2))
    val lanes = (xml \ "Run" \ "AlignToPhiX" \\ "Lane").map(_.text.toInt)
    // get data from each lane
    lanes//.take(1) // TODO :: remove take(1)
      .flatMap(l => readLane(l, fout))
  }
}

object test {
  def main(args: Array[String]) {
    Reader.readSampleNames
    val w = Reader.getAllJobs

    w.foreach(Reader.process)
    FP.env.execute
  }
}
