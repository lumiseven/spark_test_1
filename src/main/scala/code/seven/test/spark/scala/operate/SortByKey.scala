package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SortByKey")
    val sc = new SparkContext(conf)

    val rdd2 = sc.makeRDD(Array(("a", 2), ("z", 0), ("c", 1), ("b", -1)))
    rdd2.sortByKey(true).collect.foreach(println)
    rdd2.sortByKey(false).collect.foreach(println)
  }
}
