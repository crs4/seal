resolvers += "jitpack" at "https://jitpack.io"

import it.crs4.tools.avsc2java.makeSources
import it.crs4.tools.promptHadoop

lazy val hver = promptHadoop.ask

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hver , //"2.7.1" ,
  "com.github.HadoopGenomics" % "Hadoop-BAM" % "ac650efd344a74e4c6b4ca1870a9df50493a2cd9" ,
  "com.twitter" % "parquet-avro" % "1.6.0rc4" ,
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.6.1"
)

sourceGenerators in Compile += Def.task {
  makeSources((sourceManaged in Compile).value / "")
}.taskValue

name := "seal"
version := "0.1"
maintainer := "Your Name <your@email.com>"
packageSummary := "The name you want displayed in package summaries"
packageDescription := " A description of your project"

enablePlugins(JavaAppPackaging)
