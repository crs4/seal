

////////////////////////////////////////
// metadata
version := projectVersion
organization := "it.crs4"
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

val defaultHadoopVersion = "2.6.4"
val defaultFlinkVersion = "1.1.2"

////////////////////////////////////////
// main project

////////////////////////////////////////
//resolvers += Resolver.mavenLocal



////////////////////////////////////////
// imports
import sbt.Package.ManifestAttributes
import it.crs4.tools.avsc2java
import it.crs4.tools.avdl2schema
import it.crs4.tools.buildUtils
import java.io.File

lazy val hadoopVersion = Option(System.getProperty("hadoop.version")).getOrElse(defaultHadoopVersion)

// FIXME:  set devel version based on current build
lazy val projectVersion = Option(System.getProperty("seal.version")).getOrElse("devel-" ++ "xyz")


////////////////////////////////////////
// dependencies
libraryDependencies ++= Seq(
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.6.1",
  "com.google.guava" % "guava" % "19.0",
  "org.apache.avro" % "avro-mapred" % "1.7.6",
  "org.apache.flink" % "flink-shaded-hadoop2" % defaultFlinkVersion,
  "org.apache.flink" %% "flink-clients" % defaultFlinkVersion, // % "provided" ,
  "org.apache.flink" %% "flink-scala" % defaultFlinkVersion , // % "provided" ,
  "org.apache.flink" %% "flink-streaming-scala" % defaultFlinkVersion, // % "provided"
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.parquet" %  "parquet-avro" % "1.8.1",
  "org.apache.parquet" %  "parquet-hadoop" % "1.8.1",
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.seqdoop" % "hadoop-bam" % "7.2.1"
)

excludeDependencies ++= Seq(
  SbtExclusionRule("com.google.code.findbugs", "jsr305"),
  SbtExclusionRule("com.google.code.gson", "gson"),
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
  SbtExclusionRule("org.apache.flink", "flink-shaded-hadoop1_2.10") ,
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

// declare a temp directory and add it to the "clean" list
val tmpDir = baseDirectory { base => base / "temp" }
cleanFiles <+= tmpDir


////////////////////////////////////////
// generate java classes from avro schemas
sourceGenerators in Compile += Def.task {

	val tmpAvdlDir = tmpDir.value / "avdls"
	val tmpSchemaDir = tmpDir.value / "avschemas"

	// extract avdl files from bdg-formats jar
	val bdgFormatsJar = (update in Compile).value
		.select(configurationFilter("compile"))
		.filter(_.name.contains("bdg-formats"))
		.head
	buildUtils.extractAvdls(bdgFormatsJar, tmpAvdlDir)

	// generate avro schemas from avro IDLs
	avdl2schema.makeAvsc(
		buildUtils.scanDir(tmpAvdlDir) ++ buildUtils.scanDir(new File("avdl")),
		tmpSchemaDir
	)

	// Now generate java sources for the entities defined in the schemas.
	// The schema files are in the SBT's sourceManaged directory and
	// in the tmpSchemaDir we created in the previous step.
  avsc2java.compileSchemas((sourceManaged in Compile).value, tmpSchemaDir)
}.taskValue

lazy val junit2 = "com.novocode" % "junit-interface" % "0.11"

libraryDependencies += junit2 % Test

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
