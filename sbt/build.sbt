
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
    autoScalaLibrary := false,
    publishArtifact in (Compile, packageDoc) := false ,
    publishArtifact in packageDoc := false ,
    sources in (Compile,doc) := Seq.empty
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
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.seqdoop" % "hadoop-bam" % "7.2.1",
  "org.apache.parquet" %  "parquet-avro" % "1.8.1",
  "org.apache.parquet" %  "parquet-hadoop" % "1.8.1",
  "org.apache.avro" % "avro-mapred" % "1.7.6"
)

excludeDependencies ++= Seq(
  SbtExclusionRule("com.google.code.findbugs", "jsr305"),
  SbtExclusionRule("com.google.code.gson", "gson"),
  SbtExclusionRule("com.google.guava", "guava"),
  SbtExclusionRule("com.google.inject", "guice"),
  SbtExclusionRule("com.sun.jersey", "jersey-client"),
  SbtExclusionRule("com.sun.jersey", "jersey-core"),
  SbtExclusionRule("com.sun.jersey", "jersey-json"),
  SbtExclusionRule("com.sun.jersey", "jersey-server"),
  SbtExclusionRule("com.sun.jersey.contribs", "jersey-guice"),
  SbtExclusionRule("com.thoughtworks.paranamer", "paranamer"),
  SbtExclusionRule("commons-beanutils", "commons-beanutils"),
  SbtExclusionRule("commons-beanutils", "commons-beanutils-core"),
  SbtExclusionRule("io.netty", "netty"),
  SbtExclusionRule("it.unimi.dsi", "fastutil"),
  SbtExclusionRule("javax.activation", "activation"),
  SbtExclusionRule("javax.inject", "javax.inject"),
  SbtExclusionRule("javax.servlet", "servlet-api"),
  SbtExclusionRule("javax.xml.bind", "jaxb-api"),
  SbtExclusionRule("javax.xml.stream", "stax-api"),
  SbtExclusionRule("jline", "jline"),
  SbtExclusionRule("org.apache.ant", "ant"),
  SbtExclusionRule("org.apache.ant", "ant-launcher"),
  SbtExclusionRule("org.apache.avro", "avro-ipc"),
  SbtExclusionRule("org.apache.commons", "commons-jexl"),
  SbtExclusionRule("org.apache.commons", "commons-math3"),
  SbtExclusionRule("org.apache.curator", "curator-client"),
  SbtExclusionRule("org.apache.curator", "curator-framework"),
  SbtExclusionRule("org.apache.curator", "curator-recipes"),
  SbtExclusionRule("org.apache.hadoop", "hadoop-hdfs"),
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-server-common"),
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-server-common"),
  SbtExclusionRule("org.apache.parquet", "parquet-jackson"),
  SbtExclusionRule("org.apache.velocity", "velocity"),
  SbtExclusionRule("org.apache.zookeeper", "zookeeper"),
  SbtExclusionRule("org.codehaus.jackson", "jackson-jaxrs"),
  SbtExclusionRule("org.codehaus.jackson", "jackson-mapper-asl"),
  SbtExclusionRule("org.codehaus.jackson", "jackson-xc"),
  SbtExclusionRule("org.codehaus.jettison", "jettison"),
  SbtExclusionRule("org.htrace", "htrace-core"),
  SbtExclusionRule("org.mortbay.jetty", "jetty"),
  SbtExclusionRule("org.mortbay.jetty", "jetty-util"),
  SbtExclusionRule("org.mortbay.jetty", "servlet-api"),
  SbtExclusionRule("org.sonatype.sisu.inject", "cglib")
)

////////////////////////////////////////
// generate java classes from avro schemas
sourceGenerators in Compile += Def.task {
  makeSources((sourceManaged in Compile).value / "")
}.taskValue

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

////////////////////////////////////////
// disable docs generation
publishArtifact in (Compile, packageDoc) := false
publishArtifact in packageDoc := false
sources in (Compile,doc) := Seq.empty

////////////////////////////////////////
// Add "analytics-utils/version-report" command to print dependencies:
//   sbt analytics-utils/version-report
// from https://groups.google.com/d/msg/simple-build-tool/rcPh-lWbDtk/c9P-CeSR5PwJ
lazy val versionReport = TaskKey[String]("version-report")

versionReport <<= (externalDependencyClasspath in Compile, streams) map {
   (cp: Seq[Attributed[File]], streams) =>
     val report = cp.map {
       attributed =>
         attributed.get(Keys.moduleID.key) match {
           case Some(moduleId) => "%40s %20s %10s %10s".format(
             moduleId.organization,
             moduleId.name,
             moduleId.revision,
             moduleId.configurations.getOrElse("")
           )
           case None           =>
             // unmanaged JAR, just
             attributed.data.getAbsolutePath
         }
     }.mkString("\n")
     streams.log.info(report)
     report
  }
