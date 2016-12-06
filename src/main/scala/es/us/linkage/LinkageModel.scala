package es.us.linkage

/**
  * Created by Josem on 15/11/2016.
  */
class LinkageModel(private var clusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]]) extends Serializable {

  def getClusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]] = clusters

  def setCClusters(clusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]]): this.type = {
    this.clusters = clusters
    this
  }


  def printSchema(separator: String): Unit = {
    println(this.getClusters
      .toList
      .sortBy(_._1)
      .map(x => s"${x._1},${x._2.head._1},${x._2.head._2}")
      .mkString(separator))

  }

  def saveSchema: Seq[String] = {
    this.getClusters
      .toSeq
      .sortBy(_._1)
      .map(x => s"${x._1},${x._2.head._1},${x._2.head._2}")

  }


}
