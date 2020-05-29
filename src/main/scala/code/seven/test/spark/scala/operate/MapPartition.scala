package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object MapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartition")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 21, 3)
    println(rdd.getNumPartitions)

    val mapPartitionRDD = rdd.mapPartitions(array => {
      array.map(_ * 2)
    })
    mapPartitionRDD.foreach(println)
  }
}
