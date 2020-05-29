package code.seven.test.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object S1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("s1")
    val sc = new SparkContext(conf)

//    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val fileDRR = sc.textFile("in", 3)
    fileDRR.saveAsTextFile("output")
  }
}
