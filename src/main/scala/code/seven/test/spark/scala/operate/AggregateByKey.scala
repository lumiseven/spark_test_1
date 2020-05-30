package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKey")
    val sc = new SparkContext(conf)

    var listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 2), ("d", 4), ("a", 2)))
    var aggrRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(_+_,_+_)//分区内相加，分区间也相加
    aggrRDD.foreach(println)
    sc.stop()
  }
}
