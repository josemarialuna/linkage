package es.us.linkage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MainReadFolder {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Linkage")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fileOriginal = "C:\\datasets\\trabajadores.csv"
    val fileOriginalMin = "C:\\datasets\\trabajadores-min.csv"
    val dirDistances = "C:\\datasets\\distances"


    //val distances = sc.textFile(dirDistances).map(_.toDouble)

    //println(distances.count())

    val distances: RDD[Double] = sc.parallelize(Seq(
      (0.0), (1.0), (3.0), (5.0), (6.0), (1.0), (0.0), (3.0), (7.0), (8.0), (3.0), (3.0), (0.0), (7.0), (8.0), (5.0), (7.0), (7.0), (0.0), (2.0), (6.0), (8.0), (8.0), (2.0), (0.0)))

    val data = sc.parallelize(Seq(
      new Cluster(List(0)),
      new Cluster(List(1)),
      new Cluster(List(2)),
      new Cluster(List(3)),
      new Cluster(List(4))))

    //min,max,avg
    val linkage = new Linkage(4, "min")

    //println(linkage.prueba(new Cluster(List(3, 2)), new Cluster(List(1)), distances.zipWithIndex().map(_.swap).collectAsMap))

    val model = linkage.runAlgorithm(data, distances.zipWithIndex().map(_.swap).collectAsMap())


    println("RESULTADO: ")
    model.printSchema()



    sc.stop()
  }
}