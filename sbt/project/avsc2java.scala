package it.crs4.tools

import java.io.{File, PrintWriter}
import org.apache.avro.compiler.specific.SpecificCompiler

import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl
import scala.collection.JavaConversions._

import sbt.IO
import sbt.NameFilter

class ExactFilter(val matchName: String) extends NameFilter {
	def accept(name : String) = name == matchName
}

object buildUtils {
  def exactFilter(matchName : String) = new NameFilter {
    def accept(name : String) = name == matchName
  }

  def endsWithFilter(matchName : String) = new NameFilter {
    def accept(name : String) = name.endsWith(matchName)
  }

  def extractAvdls(jar : File, destDir : File) : Set[File] = {
    sbt.IO.unzip(jar, destDir, endsWithFilter(".avdl"))
  }

  def scanDir(dir : File) : Seq[File] = {
    val all = dir.listFiles
    all.filter((f:File) => (f.isFile && ! f.getName.startsWith("."))) ++
      all.filter(_.isDirectory).flatMap(scanDir)
  }
}

object avsc2java {
  def compileSchemas(outputDir : File, schemaDir : File) : Seq[File] = {
    val sFiles = schemaDir.listFiles
    // There is a method SpecificCompiler.compileSchema that accepts an array of
    // source files, rather than a single one.  However, it results in a
    // SchemaParseException because of a redefined
    // org.bdgenomics.formats.avro.StructuralVariantType type.  Compiling the
    // idls one at a time avoids the issue.
    sFiles.foreach( SpecificCompiler.compileSchema(_, outputDir) )
    buildUtils.scanDir(outputDir)
  }
}

object avdl2schema {
  private def makeOne(outdir : File, idlFile : File) : Seq[File] = {
    System.out.println("Generating schemas from " + idlFile.getName())
    val parser = new Idl(idlFile)
    val types = parser.CompilationUnit.getTypes

    // map with side effects:  writes the schema files resulting from idl
    types.map(schema => {
      val file = new File(outdir + "/" + schema.getName() + ".avsc")
      val writer = new PrintWriter(file)
      writer.write(schema.toString)
      writer.close
      file
    }).toSeq
  }

  def makeAvsc(idls : Seq[File], outdir : File) : Seq[File] = {
    outdir.mkdirs
    idls.flatMap(makeOne(outdir, _))
  }
}
