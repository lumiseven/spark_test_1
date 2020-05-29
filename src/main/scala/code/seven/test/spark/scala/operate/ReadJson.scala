package code.seven.test.spark.scala.operate

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, SparkContext}

object ReadJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadJson")
    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile("in/user.json")
//    fileRDD.foreach(println)

    val mapper = new ObjectMapper()
//    mapper.registerModule(DefaultScalaModule)
    fileRDD.map(j => mapper.writeValueAsString(j)).foreach(println)
  }
}