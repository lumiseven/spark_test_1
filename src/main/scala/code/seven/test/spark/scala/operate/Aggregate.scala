package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 3)
    val result: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result)

    sc.stop()
  }
}
