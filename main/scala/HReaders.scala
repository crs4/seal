package bclconverter.hreaders

import bclconverter.{FlinkProvider => FP}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.api.scala._
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}

object BCL {
  val toBAr : Array[Byte] = Array('A', 'C', 'G', 'T')
  def toB(b : Byte) : Byte = {
    val idx = b & 0x03
    toBAr(idx.toByte)
  }
  def toQ(b : Byte) : Byte = {
    (0x40 + ((b & 0xFC) >> 2)).toByte
  }
  def filtro(in : DataSet[(LongWritable, Array[Byte])]) : DataSet[(LongWritable, Array[Byte])] = {
    val inf = 10000000000l
    val b1 = in.map(a => (a._1, a._2.map(BCL.toB)))
    val b2 = in.map(a => (new LongWritable(2*inf + a._1.get), a._2.map(BCL.toQ)))
    val head = FP.env.fromElements((new LongWritable(-inf), "id:blabla\n".getBytes))
    val mid = FP.env.fromElements((new LongWritable(inf), "\n+mezzo\n".getBytes))
    val tail = FP.env.fromElements((new LongWritable(3*inf), "\n".getBytes))
    b1.union(b2).union(head).union(mid).union(tail)
  }
}


class BHout extends HFileOutputFormat[LongWritable, Array[Byte]] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[LongWritable, Array[Byte]] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(pos: LongWritable, what: Array[Byte]) = {
      fileOut.write(what)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[LongWritable, Array[Byte]] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)
    
    new RW
  }
}


class BHin extends HFileInputFormat[LongWritable, Array[Byte]] {
  class RR extends RecordReader[LongWritable, Array[Byte]] {
    val bsize = 1024*1024
    var buf = new Array[Byte](bsize)
    var start = -1l
    var pos = -1l
    var end = -1l
    var fileIn : FSDataInputStream = null

    def initialize(isplit: InputSplit, ta: TaskAttemptContext) = {
      val split = isplit.asInstanceOf[FileSplit]
      println(s"SPLIT size: ${split.getLength}")
      val job = ta.getConfiguration
      start = split.getStart
      pos = start
      end = start + split.getLength
      val file = split.getPath
      val fs = file.getFileSystem(job)
      fileIn = fs.open(file)
      fileIn.seek(start)
    }

    def getCurrentKey : LongWritable = {
      new LongWritable(pos)
    }

    def getProgress : Float = {
      if (start == end)
        return 1.0f

      return (fileIn.getPos - start) / (end - start).toFloat
    }

    def close = {
    }

    def getCurrentValue : Array[Byte] = {
      buf
    }

    def nextKeyValue : Boolean = {
      if (pos != end) {
        val r = fileIn.read(buf)
        if (r != bsize)
          buf = buf.take(r)
        pos += r
        return (r > 0)
      }
      else
        false
    }
  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[LongWritable, Array[Byte]] = {
    new RR
  }

  // override def isSplitable(context : JobContext, file : HPath) : Boolean = true
}


object Test {
  val root = "/home/cesco/dump/data/t/"
  val fin = root + "huge"
  val fout = root + "out"

  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(1)

    val job = Job.getInstance(FP.conf)

    val hin = new HadoopInputFormat(new BHin, classOf[LongWritable], classOf[Array[Byte]], job)
    val hipath = new HPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1024*1024)
    val d = FP.env.createInput(hin) // DataSource [(LongWritable, Array[Byte])]

    val hout = new HadoopOutputFormat(new BHout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)


    BCL.filtro(d)
      .sortPartition(0, Order.ASCENDING)
      .output(hout)//.setParallelism(1) //d.writeUsingOutputFormat(hout)
    FP.env.execute

    // hout.finalizeGlobal(1)
  }
}


