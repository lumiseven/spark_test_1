package code.seven.test.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCount-java");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> collect = jsc.textFile("in/word.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();
        collect.forEach(System.out::println);
    }
}
