package bclconverter.hreader

import org.apache.flink.streaming.api.scala._
import bclconverter.{FlinkStreamProvider => FP}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.flink.api.common.functions.MapFunction


object BCL {
  def toBQ(b : Byte) : (Byte, Byte) = {
    return((b & 0x03).toByte, ((b & 0xFC) >> 2).toByte)
  }
}

case class BCL(b : Byte) {
}


class BHout extends HFileOutputFormat[Void, Array[Byte]] {
  var fileOut : FSDataOutputStream = null
  class RW extends RecordWriter[Void, Array[Byte]] {
    def close(ta : TaskAttemptContext) = {
      fileOut.close
    }
    def write(void: Void, what: Array[Byte]) = {
      fileOut.write(what)
    }
  }
  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Void, Array[Byte]] = {
    val conf = job.getConfiguration
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(conf)
    fileOut = fs.create(file, false)

    new RW
  }
}


class BHin extends HFileInputFormat[Void, Array[Byte]] {
  class RR extends RecordReader[Void, Array[Byte]] {
    def void: Void = null
    val bsize = 1024*1024
    var cvalue = new Array[Byte](bsize)
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

    def getCurrentKey : Void = {
      void
    }

    def getProgress : Float = {
      if (start == end)
        return 1.0f

      return (fileIn.getPos - start) / (end - start).toFloat
    }

    def close = {
    }

    def getCurrentValue : Array[Byte] = {
      cvalue
    }

    def nextKeyValue : Boolean = {
      if (pos != end) {
        val r = fileIn.read(cvalue)
        pos += r
        return (r > 0)
      }
      else
        false
    }
  }

  def createRecordReader(split: InputSplit, ta: TaskAttemptContext) : RecordReader[Void, Array[Byte]] = {
    new RR
  }
}


class Filtro extends MapFunction[Array[Byte], Array[Byte]]{
  def map(in : Array[Byte]) : Array[Byte] = {
    in.map(b => (b & 0x12).toByte)
  }
}


object HReader {
  val fin = "/tmp/t/huge"
  val fout = "/tmp/t/out"

  // test
  def main(args: Array[String]) {
    def void: Void = null

    FP.env.setParallelism(2)

    val job = Job.getInstance(FP.conf)

    val hin = new HadoopInputFormat(new BHin, classOf[Void], classOf[Array[Byte]], job)
    val hipath = new HPath(fin)
    HFileInputFormat.addInputPath(job, hipath)
    // HFileInputFormat.setMaxInputSplitSize(job, 1024*1024)
    val d = FP.env.createInput(hin) // DataStream[(Void, Array[Byte])]

    val hout = new HadoopOutputFormat(new BHout, job)
    val hopath = new HPath(fout)
    HFileOutputFormat.setOutputPath(job, hopath)

    val bsize = 256

    val sin = d.map(x => x._2)
      .map(new Filtro)
    val sout = sin.map(x => (void, x))


    sout.writeUsingOutputFormat(hout)
    FP.env.execute

    hout.finalizeGlobal(1)
  }
}


