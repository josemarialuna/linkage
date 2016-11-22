package es.us.linkage

/**
  * Created by Josem on 15/11/2016.
  */
class LinkageModel(private var clusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]]) extends Serializable {

  import org.apache.spark.mllib.linalg.Vector

  def getClusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]] = clusters

  def setCClusters(clusters: scala.collection.mutable.Map[Long, Seq[(Int, Int)]]): this.type = {
    this.clusters = clusters
    this
  }


  def printSchema(): Unit = {
    println(this.getClusters.mkString(";"))
  }

}
