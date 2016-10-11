
/* Copyright (C) 2011-2016 CRS4.
 *
 * This file is part of Seal.
 *
 * Seal is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * Seal is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with Seal.  If not, see <http://www.gnu.org/licenses/>.
 */


package bclconverter.bclreader

import akka.pattern.ask
import akka.util.Timeout
import bclconverter.Fenv
import java.io.OutputStream
import java.util.concurrent.Executors
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.{ZlibCompressor, ZlibFactory}
import scala.collection.parallel._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.io.Source
import scala.xml.{XML, Node}

import Reader.Block

class Fout(filename : String) extends OutputFormat[Block] {
  var writer : OutputStream = null
  var out : FSDataOutputStream = null
  def close() = {
    writer.close
    out.close
  }
  def configure(conf : Configuration) = {
  }
  def open(taskNumber : Int, numTasks : Int) = {
    val compressor = new ZlibCompressor(
      ZlibCompressor.CompressionLevel.BEST_SPEED,
      ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
      ZlibCompressor.CompressionHeader.GZIP_FORMAT,
      64 * 1024)

    val ccf = new CompressionCodecFactory(new HConf)
    val codec = ccf.getCodecByName("gzip")
    val path = new HPath(filename + codec.getDefaultExtension)
    val fs = Reader.MyFS(path)
    if (fs.exists(path)) 
      fs.delete(path, true)
    out = fs.create(path)
    
    writer = codec.createOutputStream(out, compressor)
  }
  def writeRecord(rec : Block) = {
    writer.write(rec)
  }
}


class fuzzyIndex(sm : Map[(Int, String), String], mm : Int, undet : String) extends Serializable {
  def hamDist(a : String, b : String) : Int = {
    val va = a.getBytes
    val vb = b.getBytes
    var cow = 0
    va.indices.foreach(i => if (va(i) != vb(i)) cow += 1)
    cow
  }
  def findMatch(k : (Int, String)) : String = {
    val (lane, pat) = k
    val m = inds.filter(_._1 == lane)
      .map(x => (x, hamDist(pat, x._2)))
      .filter(_._2 <= mm).toArray.sortBy(_._2)
    val r = {
      // no close match
      if (m.isEmpty)
	undet
      else // return closest match
	m.head._1._2
      }
    seen += (k -> r)
    return r
  }
  def getIndex(k : (Int, String)) : String = {
    seen.getOrElse(k, findMatch(k))
  }
  // main
  val inds = sm.keys
  var seen = Map[(Int, String), String]()
  inds.foreach(k => seen += (k -> k._2))
}

class RData extends Serializable{
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var fuz : fuzzyIndex = null
  // parameters
  var root : String = null
  var fout : String = null
  var bdir = "Data/Intensities/BaseCalls/"
  var adapter : String = null
  var bsize = 2048
  var mismatches = 1
  var undet = "Undetermined"
  var jnum = 1
  var flinkpar = 1
  def setParams(param : ParameterTool) = {
    root = param.getRequired("root")
    fout = param.getRequired("fout")
    bdir = param.get("bdir", bdir)
    adapter = param.get("adapter", adapter)
    bsize = param.getInt("bsize", bsize)
    mismatches = param.getInt("mismatches", mismatches)
    undet = param.get("undet", undet)
    jnum = param.getInt("jnum", jnum)
    flinkpar = param.getInt("flinkpar", flinkpar)
  }
}

object Reader {
  type Block = Array[Byte]
  def MyFS(path : HPath = null) : FileSystem = {
    var fs : FileSystem = null
    val conf = new HConf
    if (path == null)
      fs = FileSystem.get(conf)
    else {
      fs = FileSystem.get(path.toUri, conf);
    }
    // return the filesystem
    fs
  }
}

class Reader extends Serializable{
  val rd = new RData
  var sampleMap = Map[(Int, String), String]()
  // process tile
  def process(input : Seq[(Int, Int)]) = {
    val mFP = new Fenv
    mFP.env.setParallelism(rd.flinkpar)
    def procReads(input : (Int, Int)) : Seq[(DataStream[Block], OutputFormat[Block])] = {
      val (lane, tile) = input
      println(s"Processing lane $lane tile $tile")
      val in = mFP.env.fromElements(input)
      val bcl = in.flatMap(new readBCL(rd)).split{
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
	  case (k, pref) => ((k._1, k._2) -> new Fout(f"${rd.fout}${pref}/${pref}_L${k._1}%03d_${tile}-${rr}.fastq"))
        }
      }
      rreads.indices.foreach{
        i => (houts(i) += (lane, rd.undet) -> new Fout(f"${rd.fout}${rd.undet}/${rd.undet}_L${lane}%03d_${tile}-${rreads(i)}.fastq"))
      }
      val stuff = rreads.indices.map{ i =>
        bcl.select(rreads(i))
          .map(x => (x._1, x._3, x._4))
          .split{
	  input : (Block, Block, Block) =>
	  new String(input._1) match {
            case x => List(rd.fuz.getIndex((lane, x)))
	  }
        }
      }
      val output = rreads.indices.flatMap{ i =>
        houts(i).keys.map{ k =>
	  val ds = stuff(i).select(k._2).map(x => (x._2, x._3))
	    .map(new toFQ)
	  // .map(new delAdapter(rd.adapter.getBytes))
	    .map(new Flatter)
	  val ho = houts(i)(k)
	  (ds, ho)
        }
      }
      return output
    }
    val stuff = input.flatMap(procReads)
    stuff.foreach(x => x._1.writeUsingOutputFormat(x._2).setParallelism(1))
    mFP.env.execute
  }
  // process tile, PRQ output
  def PRQprocess(input : Seq[(Int, Int)]) = {
    val mFP = new Fenv
    mFP.env.setParallelism(rd.flinkpar)
    def procReads(input : (Int, Int)) : Seq[(DataStream[Block], OutputFormat[Block])] = {
      val (lane, tile) = input
      println(s"Processing lane $lane tile $tile")
      val in = mFP.env.fromElements(input)
      val bcl = in.flatMap(new PRQreadBCL(rd))

      var houts = sampleMap.filterKeys(_._1 == lane)
          .map {
	  case (k, pref) => ((k._1, k._2) -> new Fout(f"${rd.fout}${pref}/${pref}_L${k._1}%03d_${tile}.prq"))
        }
      houts += (lane, rd.undet) -> new Fout(f"${rd.fout}${rd.undet}/${rd.undet}_L${lane}%03d_${tile}.prq")
      val stuff = bcl
        .split {
	input : (Block, Block, Block, Block) =>
	new String(input._1) match {
          case x => List(rd.fuz.getIndex((lane, x)))
	}
      }
      val output = houts.keys.map{ k =>
	val ds = stuff.select(k._2).map(x => (x._2, x._3, x._4))
	  .map(new PRQtoFQ)
	  .map(new PRQFlatter)
	val ho = houts(k)
	(ds, ho)
      }.toSeq
      return output
    }
    val stuff = input.flatMap(procReads)
    stuff.foreach(x => x._1.writeUsingOutputFormat(x._2).setParallelism(1))
    mFP.env.execute
  }
  def readSampleNames = {
    // open runinfo.xml
    val path = new HPath(rd.root + "SampleSheet.csv")
    val fs = Reader.MyFS(path)
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
    csv.foreach(l => sampleMap += (l(0).toInt, l(6)) -> l(1))
    rd.fuz = new fuzzyIndex(sampleMap, rd.mismatches, rd.undet)
    in.close
  }
  def getAllJobs : Seq[(Int, Int)] = {
    // open runinfo.xml
    val xpath = new HPath(rd.root + "RunInfo.xml")
    val fs = Reader.MyFS(xpath)
    val xin = fs.open(xpath)
    val xsize = fs.getFileStatus(xpath).getLen
    val xbuf = new Array[Byte](xsize.toInt)
    val xml = XML.load(xin)
    // read parameters
    val instrument = (xml \ "Run" \ "Instrument").text
    val runnum = (xml \ "Run" \ "@Number").text
    val flowcell = (xml \ "Run" \ "Flowcell").text
    rd.header = s"@$instrument:$runnum:$flowcell:".getBytes
    // reads and indexes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    rd.ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    rd.index = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "Y").map(x => Range(x._1, x._2))

    val layout = xml \ "Run" \ "FlowcellLayout"
    val lanes = (layout \ "@LaneCount").text.toInt
    val surfaces = (layout \ "@SurfaceCount").text.toInt
    val swaths = (layout \ "@SwathCount").text.toInt
    val tiles = (layout \ "@TileCount").text.toInt

    val r = for (l <- 1 to lanes; sur <- 1 to surfaces; sw <- 1 to swaths; t <- 1 to tiles) yield (l, sur * 1000 + sw * 100 + t)
    xin.close
    r
  }
}

object test {
  def main(args: Array[String]) {
    val propertiesFile = "conf/bclconverter.properties"
    val param = ParameterTool.fromPropertiesFile(propertiesFile)

    val numTasks = param.getInt("numTasks") // concurrent flink tasks to be run
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val reader = new Reader
    reader.rd.setParams(param)
    reader.readSampleNames

    def runUpTo(what : Seq[(Int, Int)]) = {
      val max = 5
      var rep = 0
      while (rep < max) {
	try {
	  reader.PRQprocess(what)  // use PRQprocess to generate PRQ files
	  rep = max
	} catch {
	  case e : Exception => {
	    rep += 1
	    if (rep == max) {
	      println("Error: Ooops, could not recover")
	      throw new Error(s"Job caused errors for $max times, aborting")
	    }
	    else
	      println(s"Warning: Exception detected, trying to recover, attempt $rep")
	  }
	}
      }
    }
    
    val w = reader.getAllJobs
    val jnum = reader.rd.jnum
    val tasks = w.sliding(jnum, jnum).map(x => Future{runUpTo(x)})
    val aggregated = Future.sequence(tasks)
    Await.result(aggregated, Duration.Inf)
  }
}
