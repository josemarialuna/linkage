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

  //Devuelve el cluster al que pertenece el punto, o el punto en el caso en que no esté
  def getRealPoint(point: Int): Int = {
    var res = point
    var auxPoint = point
    var found = false
    val default = (-1, "")
    while (!found) {
      val aux = this.clusters
        .find(x => (x._2.head._1 == res || x._2.head._2 == res))
        .getOrElse(default)._1
        .asInstanceOf[Number].intValue()
      if (aux == -1) {
        found = true
      } else {
        res = aux
      }
    }
    res
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
