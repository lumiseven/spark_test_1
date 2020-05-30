package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionWithIndex")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 21, 3)
    println(rdd.getNumPartitions)

    val mapPRDD = rdd.mapPartitionsWithIndex((i, array) => {
      array.map(i + "_" + _)
    })
    mapPRDD.foreach(println)

    sc.stop()
  }
}
