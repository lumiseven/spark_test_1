package code.seven.test.spark.scala.operate

import org.apache.spark.{SparkConf, SparkContext}

object Value {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Value")
    val sc = new SparkContext(conf)

//    // union operator
//    val rdd1 = sc.parallelize(1 to 5)
//    val rdd2 = sc.parallelize(5 to 10)
//    val rdd3 = rdd1.union(rdd2)
//    println("rdd3: ")
//    rdd3.collect().foreach(println)
//
//    // subtract operator
//    val rdd4 = sc.parallelize(3 until 7)
//    val rdd5 = sc.parallelize(6 until 10)
//    val rdd6 = rdd4.subtract(rdd5)
//    println("rdd6: ")
//    rdd6.collect().foreach(println)
//
//    // intersection
//    val rdd7 = sc.makeRDD(1 to 7)
//    val rdd8 = sc.makeRDD(5 to 10)
//    val rdd9 = rdd7.intersection(rdd8)
//    println("rdd9: ")
//    rdd9.collect.foreach(println)
//
//    // cartesian
//    val rdd10 = sc.makeRDD(3 to 6)
//    val rdd11 = sc.makeRDD(2 to 5)
//    val rdd12 = rdd10.cartesian(rdd11)
//    println("rdd12: ")
//    rdd12.collect.foreach(println)

    // zip
    val rdd13 = sc.makeRDD(Array(1,2,3), 3)
    val rdd14 = sc.makeRDD(List("a", "b", "c"), 3)
    val rdd15 = rdd13.zip(rdd14)
    val rdd16 = rdd14.zip(rdd13)
    rdd15.foreach(println)
    println
    rdd16.foreach(println)

    sc.stop()
  }
}
