package es.us.linkage

import org.apache.spark.{SparkConf, SparkContext}

object MainPasteFiles {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Pasting Distances")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val fileOriginal = "C:\\datasets\\distances"

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 4 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 5
    var numClusters = 2
    var strategyDistance = "min"

    if (args.size > 4) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
      strategyDistance = args(5)
    }

    val distances = sc.textFile(origen).coalesce(1,shuffle = true).saveAsTextFile(destino + "UnitedDistances-" + destino)

    sc.stop()
  }
}