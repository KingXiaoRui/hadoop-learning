package com.king.demo.sparksample;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author DKing
 * @description
 * @date 2019/6/19
 */
public class SequenceFileProcessor {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String fileName = "";

        JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(fileName, Text.class,
                IntWritable.class);
        JavaPairRDD<String, Integer> result = input.mapToPair(
                (PairFunction<Tuple2<Text, IntWritable>, String, Integer>) record
                        -> new Tuple2(record._1.toString(), record._2.get())
        );
    }
}
