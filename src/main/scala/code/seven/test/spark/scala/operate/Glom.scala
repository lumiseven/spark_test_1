package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Glom")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(1 to 21, 3)
    val glomRDD = rdd1.glom()
    glomRDD.foreach(array => {
      println(array.mkString(","))
    })
  }
}
