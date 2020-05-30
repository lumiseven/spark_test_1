package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FoldByKey")
    val sc = new SparkContext(conf)

    var listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 2), ("d", 4), ("a", 2)))
    var foldRDD: RDD[(String, Int)] = listRDD.foldByKey(0)(_+_)//分区内 分区间 相同操作
    foldRDD.foreach(println)
    sc.stop()
  }
}
