package com.king.demo.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/6/4
 */
public class CalculateWordsCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("hdfs://...");
        JavaRDD<String> words = input.flatMap(
                (FlatMapFunction<String, String>) s
                        -> (Iterator<String>) Arrays.asList(s.split(" "))
        );
        JavaPairRDD<String, Integer> wordMap = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        );
        JavaPairRDD<String, Integer> result = wordMap.reduceByKey(
                (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2
        );
        sc.parallelize(Arrays.asList(1, 2, 3, 4));                  //默认并行度
        sc.parallelize(Arrays.asList(1, 2, 3, 4), 10);    //自定义并行度

    }
}
