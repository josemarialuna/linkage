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

  def runAlgorithmOld(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

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
        case "min" =>
          //Calcula distancia
          clustersRes = matrix.min()(DistOrdering)
          println("Nuevo mínimo:" + clustersRes)

        case "max" =>
          //Calcula distancia
          clustersRes = matrix.max()(DistOrdering)

      }

      val punto1Aux = clustersRes.getIdW1
      val punto2Aux = clustersRes.getIdW2

      //Devuelve el punto o el cluster si este pertenece a uno
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
            //TODO quizás se pueda hacer filtrado de todos los puntos
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
            //TODO quizás se pueda hacer filtrado de todos los puntos
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
            //TODO quizás se pueda hacer filtrado de todos los puntos
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
    linkageModel
  }

  def runAlgorithm(distanceMatrix: RDD[Distance], numPoints: Int): LinkageModel = {

    var matrix = distanceMatrix

    val sc = distanceMatrix.sparkContext
    val cont = sc.accumulator(numPoints)

    val linkageModel = new LinkageModel(scala.collection.mutable.Map[Long, Seq[(Int, Int)]]())

    // sort by dist
    object DistOrdering extends Ordering[Distance] {
      def compare(a: Distance, b: Distance) = a.getDist compare b.getDist
    }

    var a = 0
    while (a < (numPoints - numClusters)) {
      println("ENTRO EN WHILE")

      var clustersRes: Distance = null

      println("Buscando minimo:")
      distanceStrategy match {
        case "min" =>
          matrix.collect().foreach(println(_))
          clustersRes = matrix.min()(DistOrdering)
          println(s"Nuevo mínimo: $clustersRes")


        case "max" =>
          //Calcula distancia
          clustersRes = matrix.max()(DistOrdering)

      }

      val punto1 = clustersRes.getIdW1
      val punto2 = clustersRes.getIdW2
      cont.add(1)
      val newIndex = cont.value.toLong


      println("Nuevo Cluster: " + newIndex + ":" + punto1 + "-" + punto2)

      //Se guarda en el modelo resultado
      linkageModel.getClusters += newIndex -> Seq((punto1, punto2))

      //Se elimina el punto encontrado
      matrix = matrix.filter(x => !(x.getIdW1 == punto1 && x.getIdW2 == punto2))

      //Se crea un nuevo punto siguiendo la estrategia
      distanceStrategy match {
        case "min" =>
          val rddPoints1 = matrix.filter(_.getIdW1 == punto1)
          val rddPoints2 = matrix.filter(_.getIdW1 == punto2)
          val rddUnionPoints = rddPoints1.union(rddPoints2)

          //Se comprueba cual de los dos RDD tienen más puntos
          val newPoints = if (rddPoints1.count() < rddPoints2.count()) {
            val listPoints2 = rddPoints2.map(x => (x.getIdW2, x.getDist)).collectAsMap()
            rddPoints1.map(x => new Distance(newIndex.toInt, x.getIdW2, math.min(x.getDist, listPoints2(x.getIdW2))))
          } else {
            val listPoints1 = rddPoints1.map(x => (x.getIdW2, x.getDist)).collectAsMap()
            rddPoints2.map(x => new Distance(newIndex.toInt, x.getIdW2, math.min(x.getDist, listPoints1(x.getIdW2))))
          }

          //Elimino los puntos completos
          var matrixSub = matrix.subtract(rddUnionPoints)

          //agrego puntos con el nuevo indice
          matrix = matrixSub.union(newPoints)


          matrixSub = matrixSub.filter(x => x.getIdW2 == punto1 || x.getIdW2 == punto2)
          if (matrixSub.count() > 0) {

            val matrixCartesian = matrixSub.cartesian(matrixSub)
              .filter(x => x._1.getIdW1 == x._2.getIdW1 && x._1.getIdW2 < x._2.getIdW2)

            val editedPoints = matrixCartesian.map(x => new Distance(x._1.getIdW1, newIndex.toInt, math.min(x._1.getDist, x._2.getDist)))

            matrix = matrix.subtract(matrixSub).union(editedPoints)
            editedPoints.unpersist()

          }


        case "max" =>
          //Calcula distancia
          clustersRes = matrix.max()(DistOrdering)

      }

      a += 1
      //if (a % 2 == 0)
      //matrix.checkpoint()
    }
    linkageModel
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
      case "min" =>
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


      case "max" =>
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

      case "avg" =>


    }

    res

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