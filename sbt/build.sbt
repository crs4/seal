
////////////////////////////////////////
// main project
lazy val root = project.in(file("."))
  .aggregate(hbam)
  .dependsOn(hbam)

////////////////////////////////////////
// hadoop-bam sub-project
lazy val hbam = project.in(file("Hadoop-BAM"))
  .settings(
  externalPom(Def.setting(baseDirectory.value / "pom.xml")) ,
    name := "Hadoop-BAM" ,
    version := "7.2.1" ,
    organization := "org.seqdoop" ,
    crossPaths := false ,
    autoScalaLibrary := false
  )

////////////////////////////////////////
// this pure java, no scala
crossPaths := false
autoScalaLibrary := false


////////////////////////////////////////
// imports
import sbt.Package.ManifestAttributes
import it.crs4.tools.avsc2java.makeSources
import it.crs4.tools.promptHadoop

val defaultHadoopVersion = "2.6.4"

lazy val hadoopVersion = Option(System.getProperty("hadoop.version")).getOrElse(defaultHadoopVersion)

// FIXME:  set devel version based on current build
lazy val projectVersion = Option(System.getProperty("seal.version")).getOrElse("devel-" ++ "xyz")

////////////////////////////////////////
// dependencies
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion ,
  "org.seqdoop" % "hadoop-bam" % "7.2.1",
  "org.apache.parquet" %  "parquet-avro" % "1.8.1" ,
  "org.apache.parquet" %  "parquet-hadoop" % "1.8.1" ,
  "org.apache.avro" % "avro-mapred" % "1.7.6"
)

////////////////////////////////////////
// generate java classes from avro schemas
sourceGenerators in Compile += Def.task {
  makeSources((sourceManaged in Compile).value / "")
}.taskValue

// lazy val junit = "junit" % "junit" % "4.12"

lazy val junit2 = "com.novocode" % "junit-interface" % "0.11"

libraryDependencies += junit2 % Test

////////////////////////////////////////
// metadata
version := projectVersion
organization := "CRS4"
packageOptions := Seq(ManifestAttributes(
  ("Built-By", System.getProperty("user.name")),
  ("Implementation-Title", "Seal"),
  ("Implementation-Vendor", "CRS4"),
  ("Implementation-Version", projectVersion),
  ("Specification-Title", "Seal"),
  ("Specification-Version", projectVersion)
))
name := "Seal"
version := projectVersion
maintainer := "Luca Pireddu <pireddu@crs4.it>, Francesco Versaci <cesco@crs4.it>"
packageSummary := "Seal"
packageDescription := "A suite of Hadoop-based tools to process high-through sequencing data"

////////////////////////////////////////
// plugin for "sbt stage", in order to gather jars
enablePlugins(JavaAppPackaging)

