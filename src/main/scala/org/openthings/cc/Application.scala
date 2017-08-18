package org.openthings.cc

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.simba.index.RTree
import org.apache.spark.sql.simba.spatial.Point

import scala.util.Failure

object Application {


  val usage =
    """
    Options:
      | Application --b-tree inputDirectoryName indexFileName
    """

  def main(args: Array[String]) {
    bTree("/home/cvasquez/development/data/points", "/tmp/index.ttl")
//    if (args.length == 0) println(usage) else {
//      val feedback = args.toList match {
//        case List("--b-tree", inputDirectoryName, outputFileName) => bTree(inputDirectoryName,outputFileName) ; "done"
//        case _ => s"\nInvalid Parameters. $usage"
//      }
//      println(feedback)
//    }
  }

  def bTree(directoryName: String, outputFileName: String) = {
    val directory = new File(directoryName)
    val outputStream = new FileOutputStream(outputFileName)

    println("reading:")

    val entries = scala.collection.mutable.ListBuffer.empty[(Point, Int)]
    val numbers = """\d[\w\.]+""".r

    val results = new PrintWriter(new File(outputFileName))
    for (file <- directory.listFiles.filter(_.getName.endsWith(".ttl"))) yield {
      try {
        print(file)
        val bufferedSource = scala.io.Source.fromFile(file)
        for (line <- bufferedSource.getLines) {
          numbers.findAllIn(line).toList match {
            case List(id, latitude, longitude) => {
              val point = new Point(Array(latitude.toDouble, longitude.toDouble))
              entries.append((point, id.toInt))
            }
            case _ => None
          }
        }
      }
      catch {
        case e: Throwable => Failure(e)
      }
    }

    val pointCount = entries.size
    val maxEntries = pointCount / 10000
    println(s"Point count: $pointCount")
    println(s"Max entries per fragment: $maxEntries")

    results.close()
    outputStream.close()
    val rtree = RTree.apply(entries.toArray, maxEntries)
    val writer = new PrintWriter(new File(outputFileName))
    TurtleExporter.toTurtle(rtree,writer)
    writer.close()

    println("Wrote "+outputFileName)
  }

}
