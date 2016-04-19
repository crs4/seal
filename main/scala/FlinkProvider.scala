package bclconverter

import org.apache.flink.api.scala._
import org.apache.flink.api.common.ExecutionMode._
import org.apache.hadoop.conf.{Configuration => HConf}

object FlinkProvider extends Serializable {

  lazy val conf = new HConf
  lazy val env = ExecutionEnvironment.getExecutionEnvironment
  // val env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123)


  // test
  def main(args: Array[String]) {
    val l = List("mucca1", "mucca2", "mucca3", "mucca4", "mucca5")
    val d = env.fromCollection(l)
    d.writeAsText("/tmp/t/prova")
    env.execute
  }
}


