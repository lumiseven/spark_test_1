package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)

    sc.makeRDD(1 to 3)
      .map(_+1)
      .map(_+"a")
      .foreach(println)
  }
}
