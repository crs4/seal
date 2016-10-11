
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

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, CompressionInputStream}

import Reader.Block


class PRQtoFQ extends MapFunction[(Block, Block, Block), (Block, Block, Block, Block, Block)] {
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
  def cleanIndex(b : Block) : Block = {
    blocksize = b.size
    bbin = ByteBuffer.wrap(b)
    val nB = toB
    val nQ = toQ
    // apply redundant fastq annotation
    nB.indices.foreach {i =>
      if (nQ(i) == 0x21.toByte) {
        nQ(i) = 0x23.toByte // #, 0 quality
        nB(i) = 0x4E.toByte // N, no-call
      }
    }
    nB
  }  
  def map(in : (Block, Block, Block)) : (Block, Block, Block, Block, Block) = {
    val (b1, b2, h) = in
    // block 1
    blocksize = b1.size
    bbin = ByteBuffer.wrap(b1)
    val nB1 = toB
    val nQ1 = toQ
    // block 2
    blocksize = b2.size
    bbin = ByteBuffer.wrap(b2)
    val nB2 = toB
    val nQ2 = toQ
    // apply redundant fastq annotation
    val nBs = Seq(nB1, nB2)
    val nQs = Seq(nQ1, nQ2)
    nBs.indices.foreach{l =>
      val nB = nBs(l)
      val nQ = nQs(l)
      nB.indices.foreach {i =>
        if (nQ(i) == 0x21.toByte) {
          nQ(i) = 0x23.toByte // #, 0 quality
          nB(i) = 0x4E.toByte // N, no-call
        }
      }
    }
    (h, nB1, nQ1, nB2, nQ2)
  }
}


class PRQFlatter extends MapFunction[(Block, Block, Block, Block, Block), Block] {
  val tab = "\t".getBytes
  val newl = "\n".getBytes
  def map(x : (Block, Block, Block, Block, Block)) : Block = {
    val (h, b1, q1, b2, q2) = x
    h ++ tab ++ b1 ++ tab ++ q1 ++ tab ++ b2 ++ tab ++ q2 ++ newl
  }
}


class PRQreadBCL(rd : RData) extends FlatMapFunction[(Int, Int), (Block, Block, Block, Block)] with Serializable{
  val newl = "\n".getBytes
  def flatMap(input : (Int, Int), out : Collector[(Block, Block, Block, Block)]) = {
    val (lane, tile) = input
    val h1 = rd.header.drop(1) ++ s"${lane}:${tile}:".getBytes
    val ldir = f"${rd.root}${rd.bdir}L${lane}%03d/"
    val fs = Reader.MyFS(new HPath(ldir))
    def getDirs(range : Seq[Int]) : Array[HPath] = {
      range
        .map(x => s"$ldir/C$x.1/")
        .map(new HPath(_))
        .filter(fs.isDirectory(_))
        .toArray
    }
    def getFiles(dirs : Array[HPath]) : Array[HPath] = {
      dirs
        .map(d => s"$d/s_${lane}_$tile.bcl.gz")
        .map(s => new HPath(s))
    }
    // open index
    val indexdirs = getDirs(rd.index(0))
    val indexlist = getFiles(indexdirs)
    val index = new BCLstream(indexlist, rd.bsize)
    // open bcls, filter, control and location files
    val cydirs = rd.ranges.map(getDirs)
    val flist = cydirs.map(getFiles)
    val bcls = flist.map(f => new BCLstream(f, rd.bsize))

    val fil = new Filter(new HPath(f"${rd.root}${rd.bdir}L${lane}%03d/s_${lane}_${tile}.filter"), rd.bsize)
    // val control = new Control(new HPath(f"${rd.root}${rd.bdir}L${lane}%03d/s_${lane}_${tile}.control"), rd.bsize << 1)
    val locs = new Locs(new HPath(f"${rd.root}${rd.bdir}../s.locs"), rd.bsize << 3)

    var buf : Seq[Array[Block]] = Seq(null, null)
    while ({buf = bcls.map(_.getBlock); buf(0) != null}) {
      val indbuf = index.getBlock.map((new toFQ).cleanIndex)
      buf(0).indices.foreach {
        i =>
        val ind = indbuf(i)
	val h2 = locs.getCoord
	if (fil.getFilter == 1.toByte)
          out.collect(ind, buf(0)(i), buf(1)(i), h1 ++ h2)
      }
    }
    bcls.foreach(_.close)
    index.close
    locs.close
    fil.close
  }
}

