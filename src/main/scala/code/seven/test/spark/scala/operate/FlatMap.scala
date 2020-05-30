package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(List(1, 2, 3), List(3, 4, 5, 6)))
    rdd.flatMap(a => a).foreach(println)
    sc.stop()
  }
}
