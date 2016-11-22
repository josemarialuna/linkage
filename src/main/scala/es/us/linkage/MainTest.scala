package es.us.linkage

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object MainTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Linkage")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fileOriginal = "C:\\datasets\\trabajadores.csv"
    val fileOriginalMin = "C:\\datasets\\trabajadores-min.csv"

    val distances = sc.parallelize(Seq(Vectors.dense(0.0, 7.0, 2.0, 3.0), Vectors.dense(7.0, 0.0, 1.0, 4.0), Vectors.dense(2.0, 1.0, 0.0, 5.0), Vectors.dense(3.0, 4.0, 5.0, 0.0)))
    val data = sc.parallelize(Seq(new Cluster(List(0)), new Cluster(List(1)), new Cluster(List(2)), new Cluster(List(3))))

    val linkage = new Linkage(1, "Min")

    //println(linkage.prueba(new Cluster(List(3, 2)), new Cluster(List(1)), distances.zipWithIndex().map(_.swap).collectAsMap))

    val model = linkage.runAlgorithm(data, distances.zipWithIndex().map(_.swap).collectAsMap())


    println("RESULTADO: ")
    model.printSchema()


  }
}