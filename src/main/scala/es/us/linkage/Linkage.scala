package es.us.linkage

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by Josem on 15/11/2016.
  */
class Linkage(
               private var numClusters: Int,
               private var distanceStrategy: String) extends Serializable {

  def getNumClusters: Int = numClusters

  def setNumClusters(numClusters: Int): this.type = {
    this.numClusters = numClusters
    this
  }

  def getDistanceStrategy: String = distanceStrategy

  def setDistanceStrategy(distanceStrategy: String): this.type = {
    this.distanceStrategy = distanceStrategy
    this
  }

  def runAlgorithm(data: RDD[Cluster],
                   distanceMatrix: scala.collection.Map[Long, Vector]): LinkageModel = {

    val sc = data.sparkContext
    val contIni = data.count()


    val cont = sc.accumulator(contIni)

    //val cartesianData = data.cartesian(data)
    var indexedData = data.zipWithIndex().map(_.swap)
    var min = 10
    //var cluster1: Int = _
    //var cluster2: Int = _

    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())
    /*
      for (i <- 0 to data.count().toInt) {
        var c1 = indexedData.lookup(i).head
        for (j <- i + 1 to data.count().toInt) {
          var c2 = indexedData.lookup(j).head
          var d = Linkage.clusterDistance(c1, c2, distanceMatrix)
          if (d < min) {
            cluster1 = i
            cluster2 = j
          }
        }
      }
  */
    var a = 0;
    while (a < 3) {
      println("ENTRO EN WHILE")

      val cartesianData = indexedData
        .cartesian(indexedData)
        .filter { case (x, y) => x != y }
        .cache()

      val minAux = cartesianData.map { case (x, y) =>
        Linkage.clusterDistance(x._2, y._2, distanceMatrix)
      }.min()
      println("Nuevo mínimo:" + minAux)


      val clustersRes = cartesianData.map { case (x, y) =>
        ((x, y), Linkage.clusterDistance(x._2, y._2, distanceMatrix))
      }.map(_.swap)
        .lookup(minAux)

      println("PAR ENCONTRADO:")

      val cluster1 = clustersRes.head._1._2
      val cluster2 = clustersRes.head._2._2
      val newIndex = cont.value
      cont.add(1)

      val c1 = clustersRes.head._1._1.toInt
      val c2 = clustersRes.head._2._1.toInt

      println(cluster1.toString)
      println(cluster2.toString)

      val resAux = cluster1.mixCluster(cluster2)

      //Crea nuevo cluster con clusters resultados
      val newCluster = Seq((newIndex, resAux))

      val newClusterRDD = sc.parallelize(newCluster)

      //Se eliminan los clusters del RDD
      val filteredData = indexedData
        .filter(x => !(cluster1.getCoordinates.contains(x._1.toInt) || cluster2.getCoordinates.contains(x._1.toInt)))

      println("NUEVOS CLUSTERS:")
      println(filteredData.map(_._1.toString).collect().mkString(";"))


      //Se inserta el nuevo cluster en el RDD
      indexedData = filteredData.union(newClusterRDD)

      //Se agregan los puntos
      linkageModel.getClusters += newIndex -> Seq((c1, c2))
      a += 1
    }
    return linkageModel
  }


  def prueba(
              c1: Cluster,
              c2: Cluster,
              distanceMatrix: scala.collection.Map[Long, Vector]): Double = {
    return Linkage.clusterDistance(c1, c2, distanceMatrix)

  }


}


object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[Long, Vector]): Double = {
    var minAux: Double = 100.0

    c1.getCoordinates.foreach { x =>
      c2.getCoordinates.foreach { y =>

        val aux = distanceMatrix(x).apply(y)
        if (aux < minAux)
          minAux = aux

      }
    }

    return minAux
  }


  //Calculate the distance between two vectors
  private def calculateDistance(
                                 v1: Vector,
                                 v2: Vector,
                                 strategy: String): Double = {
    var totalDist = 0.0
    for (z <- 1 to v1.size) {
      var minAux = 0.0
      try {
        val line = v1.apply(z)
        val linePlus = v2.apply(z)
        //El mínimo se suma a totalDist
        if (line < linePlus) {
          minAux = line
        } else {
          minAux = linePlus
        }
      } catch {
        case e: Exception => null
      } finally {
        totalDist += minAux
      }

    }
    totalDist

  }
}