package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RePartition")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 21, 4)
    println(rdd.getNumPartitions)

    val rdd2: RDD[Int] = rdd.repartition(2)

    val glomRDD: RDD[Array[Int]] = rdd2.glom()
    glomRDD.foreach(array => {
      println(array.mkString(","))
    })

    sc.stop()
  }
}
