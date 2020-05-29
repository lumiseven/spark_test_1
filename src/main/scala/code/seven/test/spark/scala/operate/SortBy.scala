package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SortBy")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(3,5,9,10,1,100,33,23,0,-11))
    val sortByRDD: RDD[Int] = rdd.sortBy(x => x)
    sortByRDD.collect.foreach(println)

    val rdd2 = sc.makeRDD(Array(("a", 2), ("c", 1), ("b", -1)))
    rdd2.sortBy(x => x._2).collect.foreach(println)
  }
}
