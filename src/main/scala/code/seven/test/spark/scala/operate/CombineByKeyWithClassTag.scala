package code.seven.test.spark.scala.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyWithClassTag {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CombineByKeyWithClassTag")
    val sc = new SparkContext(conf)

    var listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 2), ("d", 4)))

    // 使用combineByKeyWithClassTag 计算平均值
    var combine: RDD[(String, (Int, Int))] = listRDD.combineByKeyWithClassTag((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val result = combine.map{case(key,value)=>(key,value._1/value._2.toDouble)}
    result.foreach(println)

    sc.stop()
  }
}
