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

  def runAlgorithm(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

    var matrix = distanceMatrix

    val sc = distanceMatrix.sparkContext
    val cont = sc.accumulator(numPoints)

    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())

    var a = 0
    while (a < (numPoints - numClusters)) {
      println("ENTRO EN WHILE")

      var clustersRes: Distance = null

      // sort by dist
      object DistOrdering extends Ordering[Distance] {
        def compare(a: Distance, b: Distance) = a.getDist compare b.getDist
      }

      distanceStrategy match {
        case "min" => {
          //Calcula distancia
          clustersRes = matrix.min()(DistOrdering)
          println("Nuevo mínimo:" + clustersRes)

        }
        case "max" => {
          //Calcula distancia
          clustersRes = matrix.max()(DistOrdering)
        }
      }

      val punto1Aux = clustersRes.getIdW1
      val punto2Aux = clustersRes.getIdW2

      val punto1 = linkageModel.getRealPoint(punto1Aux)
      val punto2 = linkageModel.getRealPoint(punto2Aux)
      cont.add(1)
      val newIndex = cont.value.toLong


      println("Nuevo Cluster: " + newIndex + ":" + punto1 + "-" + punto2)

      //Se guarda en el modelo resultado
      linkageModel.getClusters += newIndex -> Seq((punto1, punto2))

      //se eliminan la distancia de la matriz
      if (linkageModel.isCluster(punto1)) {
        val clusterPoints1 = linkageModel.giveMePoints(punto1)
        if (linkageModel.isCluster(punto2)) {
          val clusterPoints2 = linkageModel.giveMePoints(punto2)
          for (clusterPoint1 <- clusterPoints1) {
            for (clusterPoint2 <- clusterPoints2) {
              if (clusterPoint1 < clusterPoint2) {
                matrix = matrix.filter(x => !(x.getIdW1 == clusterPoint1 && x.getIdW2 == clusterPoint2))
              } else {
                matrix = matrix.filter(x => !(x.getIdW1 == clusterPoint2 && x.getIdW2 == clusterPoint1))
              }
            }
          }
        } else {
          for (clusterPoint1 <- clusterPoints1) {
            if (clusterPoint1 < punto2) {
              matrix = matrix.filter(x => !(x.getIdW1 == clusterPoint1 && x.getIdW2 == punto2))
            } else {
              matrix = matrix.filter(x => !(x.getIdW1 == punto2 && x.getIdW2 == clusterPoint1))
            }
          }
        }
      } else {
        if (linkageModel.isCluster(punto2)) {
          val clusterPoints2 = linkageModel.giveMePoints(punto2)
          for (clusterPoint2 <- clusterPoints2) {
            if (clusterPoint2 < punto1) {
              matrix = matrix.filter(x => !(x.getIdW1 == clusterPoint2 && x.getIdW2 == punto1))
            } else {
              matrix = matrix.filter(x => !(x.getIdW1 == punto1 && x.getIdW2 == clusterPoint2))
            }
          }
        } else {
          if (punto1 < punto2) {
            matrix = matrix.filter(x => !(x.getIdW1 == punto1 && x.getIdW2 == punto2))
          } else {
            matrix = matrix.filter(x => !(x.getIdW1 == punto2 && x.getIdW2 == punto1))
          }
        }
      }



      matrix = matrix.filter(x => !(x.getIdW1 == punto1Aux && x.getIdW2 == punto2Aux))

      a += 1
      if (a % 2 == 0)
        matrix.checkpoint()
    }
    return linkageModel
  }

}


object Linkage {

  //Return the distance between two given clusters
  def clusterDistance(
                       c1: Cluster,
                       c2: Cluster,
                       distanceMatrix: scala.collection.Map[(Int, Int), Float],
                       strategy: String): Double = {
    var res = 0.0
    var aux = res

    strategy match {
      case "min" => {
        res = 100.0

        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            }
            else {
              aux = distanceMatrix(y, x)
            }
            if (aux < res)
              res = aux

          }
        }

      }
      case "max" => {
        res = 0.0
        c1.getCoordinates.foreach { x =>
          c2.getCoordinates.foreach { y =>
            //Look for just in the upper diagonal of the "matrix"
            if (x < y) {
              aux = distanceMatrix(x, y)
            } else {
              aux = distanceMatrix(y, x)
            }
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
  //DEPRECATED
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