package com.king.demo.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/6/2
 */
public class SplitWords {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("splitWords").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) s
                        -> (Iterator<String>) Arrays.asList(s.split(" "))
        );
    }
}
