package es.us.linkage

import org.apache.spark.{SparkConf, SparkContext}

object MainReadFolder {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Linkage")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val fileOriginal = "C:\\datasets\\distances"

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 4 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 5
    var numClusters = 4
    var strategyDistance = "min"

    if (args.size > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
      strategyDistance = args(5)
    }

    val distances = sc.textFile(origen, numPartitions).map(_.toDouble)

    println(distances.count())

    //val distances: RDD[Double] = sc.parallelize(Seq(
    //(0.0), (1.0), (3.0), (5.0), (6.0), (1.0), (0.0), (3.0), (7.0), (8.0), (3.0), (3.0), (0.0), (7.0), (8.0), (5.0), (7.0), (7.0), (0.0), (2.0), (6.0), (8.0), (8.0), (2.0), (0.0)) )


    val data = sc.parallelize(Cluster.createInitClusters(numPoints), numPartitions)

    //min,max,avg
    val linkage = new Linkage(numClusters,strategyDistance)

    val model = linkage.runAlgorithm(data, distances.zipWithIndex().map(_.swap).collectAsMap())

    println("RESULTADO: ")
    model.printSchema(";")

    sc.parallelize(model.saveSchema).saveAsTextFile(destino + "Linkage-" + destino)

    sc.stop()
  }
}