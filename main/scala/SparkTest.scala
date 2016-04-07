package SparkTest
import org.apache.flink.api.scala._
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader}
import org.apache.hadoop.mapreduce.{Job => HJob}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.hadoop.io.{LongWritable, ByteWritable, IntWritable}
import bclconverter.reader.{Hin, Hout}
import bclconverter.blockhadoop.{BHin, BHout}

object index {
  val fin = "/tmp/t/huge"
  val fout = "/tmp/t/out"
  type Robo = Array[Byte] // LongWritable // if using Hin, Hout


  // main
  def main(args: Array[String]) {
    val sc = SparkProvider.sparkContext

    val job = new HJob(sc.hadoopConfiguration)
    val hipath = new HPath(fin)
    // HFileInputFormat.addInputPath(job, hipath)

    val r = sc.newAPIHadoopFile(fin, classOf[BHin], classOf[Void], classOf[Robo], sc.hadoopConfiguration)

    val hopath = new HPath(fout)
    // HFileOutputFormat.setOutputPath(job, hopath)

    r.saveAsNewAPIHadoopFile(fout, classOf[Void], classOf[Robo], classOf[BHout], sc.hadoopConfiguration)

    sc.stop
  }
}
