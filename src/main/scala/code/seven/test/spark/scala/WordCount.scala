package code.seven.test.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc = new SparkContext(config)

    sc.textFile("in/word.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .foreach(println)

  }

}
