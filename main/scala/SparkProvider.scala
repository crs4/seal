package SparkTest

import scala.collection.JavaConversions._
import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkProvider {
  lazy val sparkContext : SparkContext = {
    val configObject = ConfigFactory.load()
    val map = configObject.getConfig("spark").entrySet.map(x => (x.getKey, x.getValue)).toMap//.unwrapped()
    def collapse(data : Map[String, Any], path : String, acc : Map[String, String]) : Map[String, String] = {
      data.map {
        case (key, next : java.util.Map[String, Any]) =>
          collapse(next.toMap, path+"."+key, acc)

        case (key, o : ConfigValue) => o.valueType match {
          case ConfigValueType.STRING => acc + ((path + "." + key) → o.unwrapped.toString)
	  case _ => throw new Exception("Error in spark config")
        }
      }.reduce(_ ++ _)
    }

    val conf = collapse(map.toMap, "spark", Map.empty)
    val sc = new SparkConf()
    sc.setAll(conf)
    println(" ** Spark Conf **")
    println(sc.toDebugString)
    new SparkContext(sc)
  }
}
