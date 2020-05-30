package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object MapValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapValue")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    rdd.mapValues(v => v + "_test").collect.foreach(println)

    sc.stop()
  }
}
