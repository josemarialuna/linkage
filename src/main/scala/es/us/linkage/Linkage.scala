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

    var a = 0
    while (a < numClusters) {
      println("ENTRO EN WHILE")

      var cluster1: Cluster = null
      var cluster2: Cluster = null
      var clustersRes: Seq[((Long, Cluster), (Long, Cluster))] = Nil

      val cartesianData = indexedData
        .cartesian(indexedData)
        .filter { case (x, y) => x != y }
        .cache()

      distanceStrategy match {
        case "min" => {
          //Calcula distancia
          val minAux = cartesianData.map { case (x, y) =>
            Linkage.clusterDistance(x._2, y._2, distanceMatrix, distanceStrategy)
          }.min()
          println("Nuevo mínimo:" + minAux)

          //Búsqueda del valor mínimo en los datos
          clustersRes = cartesianData.map { case (x, y) =>
            ((x, y), Linkage.clusterDistance(x._2, y._2, distanceMatrix, distanceStrategy))
          }.map(_.swap)
            .lookup(minAux)

        }
        case "max" => {
          //Calcula distancia
          val maxAux = cartesianData.map { case (x, y) =>
            Linkage.clusterDistance(x._2, y._2, distanceMatrix, distanceStrategy)
          }.max()
          println("Nuevo máximo:" + maxAux)

          //Búsqueda del valor mínimo en los datos
          clustersRes = cartesianData.map { case (x, y) =>
            ((x, y), Linkage.clusterDistance(x._2, y._2, distanceMatrix, distanceStrategy))
          }.map(_.swap)
            .lookup(maxAux)
        }
      }


      println("PAR ENCONTRADO:")

      cluster1 = clustersRes.head._1._2
      cluster2 = clustersRes.head._2._2
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
        .filter(x => c1 != x._1.toInt && c2 != x._1.toInt)

      //Se inserta el nuevo cluster en el RDD
      indexedData = filteredData.union(newClusterRDD)

      println("NUEVOS CLUSTERS:")
      println(indexedData.map(_._1.toString).collect().mkString(";"))

      //Se agregan los puntos
      linkageModel.getClusters += newIndex -> Seq((c1, c2))
      a += 1
    }
    return linkageModel
  }


  def prueba(
              c1: Cluster,
              c2: Cluster,
              distanceMatrix: scala.collection.Map[Long, Vector],
              distanceStrategy: String): Double = {
    return Linkage.clusterDistance(c1, c2, distanceMatrix, distanceStrategy)

  }


}


object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[Long, Vector],
                       strategy: String): Double = {
    var res = 0.0

    strategy match {
      case "min" => {
        res = 100.0

        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            val aux = distanceMatrix(x).apply(y)
            if (aux < res)
              res = aux

          }
        }

      }
      case "max" => {
        res = 0.0
        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>

            val aux = distanceMatrix(x).apply(y)
            if (aux > res)
              res = aux
          }
        }
      }
      case "avg" => {

      }
    }

    return res

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