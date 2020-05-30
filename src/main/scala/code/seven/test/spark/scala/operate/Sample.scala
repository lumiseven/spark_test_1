package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Sample")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,0,11,2,2,3,4,6,7,5,6,5,4,6,7,5,5,5,7,8))
    println(rdd.count())
    rdd.sample(false, 0.1).foreach(println)

    sc.stop()
  }
}
