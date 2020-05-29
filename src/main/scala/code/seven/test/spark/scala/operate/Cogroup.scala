package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Cogroup")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd2 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    rdd1.cogroup(rdd2).foreach(println)
  }
}
