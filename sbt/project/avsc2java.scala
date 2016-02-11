package it.crs4.tools

import java.io.File
import org.apache.avro.compiler.specific.SpecificCompiler

object avsc2java {
  def scanDir(dir : File) : Seq[File] = {
    val all = dir.listFiles
    all.filter(_.isFile) ++ all.filter(_.isDirectory).flatMap(scanDir)
  }

  def makeSources(base : File) : Seq[File] = {
    val sFiles = new File("avro-schemas").listFiles
    val dir = new File(base.getAbsoluteFile.getParent)
    SpecificCompiler.compileSchema(sFiles, dir)
    scanDir(dir)
  }
}
