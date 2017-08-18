package org.openthings.cc

import java.io.StringWriter

import org.apache.spark.sql.simba.index._
import org.apache.spark.sql.simba.spatial.{MBR, Point}

object TurtleExporter {

  def main(args: Array[String]): Unit = {
    buildIndexExample()
  }

  private def buildIndexExample(): Unit = {
    var entries = new Array[(Point, Int)](221)
    var cnt = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          entries(cnt) = (new Point(Array(i, j)), i + j)
          cnt = cnt + 1
        }
      }
    }
    val rtree = RTree.apply(entries, entries.size/10)

    val writer = new StringWriter()
    toTurtle(rtree,writer)
    println(writer.toString)
  }

  def toTurtle(rtree: RTree, writer: java.io.Writer):Unit = {
    writer.write("@prefix gf: <http://example.org/geofragments/> .\n")
    toTurtle(rtree.root,writer,0)
  }

  private def toTurtle(node: RTreeEntry, writer: java.io.Writer, level:Int):Unit = {
    node match {
      case leaf:RTreeLeafEntry => "" // A point
      case internal:RTreeInternalEntry => toTurtle(internal.node,writer,level)
    }
  }

  private def toTurtle(node: RTreeNode, writer: java.io.Writer, level:Int):Unit = {
    node match {
      case RTreeNode(mbr:MBR,childs,false) => {
        writer.write(getNodeRDF(node,level)+"\n")
        getRelationsRDF(node) match {
          case Some(relations) => writer.write(relations)
        }
        childs.foreach(child => toTurtle(child,writer,level+1))
      }
      case RTreeNode(mbr:MBR,_,true) => writer.write(getNodeRDF(node,level)+"\n")
    }
  }

  private def getNodeRDF(node: RTreeNode, level:Int):String = {
    node match {
      case RTreeNode(MBR(Point(Array(a,b)),Point(Array(c,d))),childs,_) => {
        val mBR = node.m_mbr
        val uri = getURI(mBR)
        val wkt = s"POLYGON (($a $b,$a $d,$c $d,$c $b,$a $b))"
        val centroidLatitude = mBR.centroid.coord(0)
        val centroidLongitude = mBR.centroid.coord(1)
        val area = mBR.area

        val numberOfPartitions = if (node.isLeaf) 0 else childs.length
        val numberOfPoints = node.size
        s"""<$uri> a gf:Box ;
              gf:wkt "$wkt" ;
              gf:centroidLatitude "$centroidLatitude" ;
              gf:centroidLongitude "$centroidLongitude" ;
              gf:area "$area" ;
              gf:zoomLevel "$level" ;
              gf:numberOfPoints "$numberOfPoints" ;
              gf:numberOfPartitions "$numberOfPartitions" ;
              gf:hasMBR "$mBR" . """
      }
    }
  }

  private def getRelationsRDF(node: RTreeNode):Option[String] = {
    val currentUri = getURI(node.m_mbr)
    val relations = node.m_child.map {
      case node: RTreeInternalEntry =>
        val childUri = getURI(node.mbr)
        s"<$currentUri> gf:hasPartition <$childUri> ."
      case node: RTreeNode => {
        val childUri = getURI(node.m_mbr)
        s"<$currentUri> gf:hasPartition <$childUri> ."
      }
    }.mkString("\n")
    if (relations.isEmpty) None else Some(relations)
  }

  /**
    * Builds an unique URI for a bounding box
    * @param mbr
    * @return
    */
  private def getURI(mbr:MBR): String = {
    val hash = mbr.hashCode()
    val unique = {
      if (hash<1) {
        math.abs(hash)+"N"
      } else hash
    }
    s"http://example.org/box/$unique"
  }

}
