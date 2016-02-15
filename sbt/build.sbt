resolvers += "jitpack" at "https://jitpack.io"

import sbt.Package.ManifestAttributes
import it.crs4.tools.avsc2java.makeSources
import it.crs4.tools.promptHadoop

val defaultHadoopVersion = "2.7.1"

lazy val hadoopVersion = Option(System.getProperty("hadoop.version")).getOrElse(defaultHadoopVersion)

// FIXME:  set devel version based on current build
lazy val projectVersion = Option(System.getProperty("seal.version")).getOrElse("devel-" ++ "xyz")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion ,
  "com.github.HadoopGenomics" % "Hadoop-BAM" % "ac650efd344a74e4c6b4ca1870a9df50493a2cd9" ,
  "com.twitter" % "parquet-avro" % "1.6.0rc4" ,
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.6.1"
)

sourceGenerators in Compile += Def.task {
  makeSources((sourceManaged in Compile).value / "")
}.taskValue


lazy val baseSettings = Defaults.defaultSettings ++ Seq(
    version := projectVersion,
    organization := "CRS4",
    packageOptions := Seq(ManifestAttributes(
                      ("Built-By", System.getProperty("user.name")),
                      ("Implementation-Title", "Seal"),
                      ("Implementation-Vendor", "CRS4"),
                      ("Implementation-Version", projectVersion),
                      ("Specification-Title", "Seal"),
                      ("Specification-Version", projectVersion)
                    )))
name := "Seal"
version := projectVersion
maintainer := "Luca Pireddu <pireddu@crs4.it>, Francesco Versaci <cesco@crs4.it>"
packageSummary := "Seal"
packageDescription := "A suite of Hadoop-based tools to process high-through sequencing data"

enablePlugins(JavaAppPackaging)
