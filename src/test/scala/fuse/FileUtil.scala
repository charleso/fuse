package fuse

import java.io.File
import java.nio.file.Files

import scala.annotation.tailrec
import scala.io.Source

object FileUtil {

  def withTempDir[A](f: File => A): A = {
    val dir = newTempDir
    try {
      f(dir)
    } finally {
      cleanUpDir(dir)
    }
  }

  def newTempDir: File =
    Files.createTempDirectory("FileUtil").toFile

  def cleanUpDir(dir: File): Unit = {
    @tailrec
    def getAllDirsAndFiles(files: Seq[File], acc: Seq[File]): Seq[File] = files match {
      case file +: rest if file.isDirectory =>
        getAllDirsAndFiles(file.listFiles ++ rest, file +: acc)
      case file +: rest =>
        getAllDirsAndFiles(rest, file +: acc)
      case Seq() =>
        acc
    }

    getAllDirsAndFiles(Seq(dir), Seq.empty).foreach(_.delete())
    dir.delete()
    ()
  }

  def readFile(path: String): String =
    scala.io.Source.fromFile(path, "UTF-8").mkString

  def writeFile(file: File, value: String): Unit = {
    val w = new java.io.PrintWriter(file, "UTF-8")
    try {
      w.write(value)
    } finally {
      w.close()
    }
  }

  def readFileTestResource(inputPath: String): String =
    Source.fromInputStream(getClass.getResourceAsStream("/" + inputPath), "UTF-8").mkString

  def copyFileFromTestResource(tempDir: File, inputPath: String): File =
    copyFileWith(tempDir, inputPath, inputPath, readFileTestResource)(identity)

  def copyFileFromTestResourceTo(tempDir: File, inputPath: String, outputPath: String): File =
    copyFileWith(tempDir, inputPath, outputPath, readFileTestResource)(identity)

  def copyFile(tempDir: File, inputPath: String): File =
    copyFileWith(tempDir, inputPath, inputPath, readFile)(identity)

  private def copyFileWith(tempDir: File, inputPath: String, outputPath: String, fileReader: String => String)(f: String => String): File = {
    val input = fileReader(inputPath)
    val outputFile = new File(tempDir, outputPath)
    outputFile.getParentFile.mkdirs()
    FileUtil.writeFile(outputFile, f(input))
    outputFile
  }
}
