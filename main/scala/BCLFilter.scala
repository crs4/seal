package bclconverter.bclreader

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}

import Reader.Block

class delAdapter(adapter : Block) extends MapFunction[(Block, Block, Block), (Block, Block, Block)] {
  val len = adapter.size
  val stringency = 0.9
  val minlen = 35
  val shortread = 22
  def map(in : (Block, Block, Block)) : (Block, Block, Block) = {
    val (h, b, q) = in
    def testPos(pos : Int) : Double = {
      val piece = b.view.slice(pos, pos + len)
      val rlen = piece.size
      var matches = 0
      var mismatches = 0
      var i = 0
      while(i < rlen) {
        val p = piece(i)
	val a = adapter(i)
	i += 1
        if (p != 0x4E.toByte){ // if N, do nothing
          if (a == p)
            matches += 1
	  else {
            mismatches += 1
	    if (mismatches > 1 && mismatches > matches)
	      return 0
	  }
        }
      }
      if (matches > 0)
	return matches.toDouble / (matches.toDouble + mismatches.toDouble)
      else
	return 0
    }
    def findMatch : Int = {
      var pos = 0
      while ((testPos(pos) < stringency) && pos < b.size)
        pos += 1
      pos
    }
    // start
    val m = findMatch
    val start = if (m < shortread) 0 else m
    Range(start, b.size).foreach{i =>
      b(i) = 0x4E.toByte // N, no-call
      q(i) = 0x23.toByte // #, 0 quality
    }
    (h, b, q)
  }
}

object toFQ {
  val dict = CreateTable.getArray
  val toBase : Block = Array('A', 'C', 'G', 'T')
}

class toFQ extends MapFunction[(Block, Block), (Block, Block, Block)] {
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
  def map(in : (Block, Block)) : (Block, Block, Block) = {
    val (b, h) = in
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
    (h, nB, nQ)
  }
}

class Flatter extends MapFunction[(Block, Block, Block), Block] {
  val mid = "\n+\n".getBytes
  val newl = "\n".getBytes
  def map(x : (Block, Block, Block)) : Block = {
    val (h, b, q) = x
    h ++ b ++ mid ++ q ++ newl
  }
}

class readBCL extends FlatMapFunction[(Int, Int), (Block, Int, Block, Block)] {
  val newl = "\n".getBytes
  def flatMap(input : (Int, Int), out : Collector[(Block, Int, Block, Block)]) = {
    val fs = FileSystem.get(new HConf)
    val (lane, tile) = input
    val h1 = Reader.header ++ s"${lane}:${tile}:".getBytes
    val h3 = Reader.ranges.indices.map(rr => s" ${rr + 1}:N:".getBytes)
    val ldir = f"${Reader.root}${Reader.bdir}L${lane}%03d/"
    def getDirs(range : Seq[Int]) : Array[HPath] = {
      range
        .map(x => s"$ldir/C$x.1/")
        .map(new HPath(_))
        .filter(fs.isDirectory(_))
        .toArray
    }
    def getFiles(dirs : Array[HPath]) : Array[HPath] = {
      dirs
        .map(d => s"$d/s_${lane}_$tile.bcl")
        .map(s => new HPath(s))
    }
    // open index
    val indexdirs = getDirs(Reader.index(0))
    val indexlist = getFiles(indexdirs)
    val index = new BCLstream(indexlist)
    // open bcls, filter, control and location files
    val cydirs = Reader.ranges.map(getDirs)
    val flist = cydirs.map(getFiles)
    val bcls = flist.map(f => new BCLstream(f))

    val fil = new Filter(new HPath(f"${Reader.root}${Reader.bdir}L${lane}%03d/s_${lane}_${tile}.filter"))
    val control = new Control(new HPath(f"${Reader.root}${Reader.bdir}L${lane}%03d/s_${lane}_${tile}.control"))
    val clocs = new Locs(new HPath(f"${Reader.root}${Reader.bdir}../L${lane}%03d/s_${lane}_${tile}.clocs"))

    var buf : Seq[Array[Block]] = Seq(null, null)
    while ({buf = bcls.map(_.getBlock); buf(0) != null}){
      val indbuf = index.getBlock.map((new toFQ).cleanIndex)
      buf(0).indices.foreach {
        i =>
        val ind = indbuf(i)
	val h2 = clocs.getCoord
        val h4 = control.getControl ++ ind ++ newl
	if (fil.getFilter == 1.toByte)
          buf.indices.foreach(rr => out.collect(ind, rr, buf(rr)(i), h1 ++ h2 ++ h3(rr) ++ h4))
      }
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
    // Transposition
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
      s"$con:".getBytes
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

class fuzzyIndex(sm : Map[(Int, String), String]) {
  val mm = Reader.mismatches
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
	Reader.undet
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
