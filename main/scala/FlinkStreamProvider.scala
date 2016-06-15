package bclconverter

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.ExecutionMode._
import org.apache.hadoop.conf.{Configuration => HConf}

object FlinkStreamProvider extends Serializable {

  lazy val conf = new HConf
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123)


  // test
  def main(args: Array[String]) {
    val l = List("mucca1", "mucca2", "mucca3", "mucca4", "mucca5")
    val d = env.fromCollection(l)
    d.writeAsText("/tmp/t/prova")
    env.execute
  }
}


class Fenv {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
}
