package com.king.demo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/5/20
 */
public class PartitionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Partition Test");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("");
        lines.mapPartitions(
                new FlatMapFunction<Iterator<String>, Object>() {
                    @Override
                    public Iterator<Object> call(Iterator<String> stringIterator) throws Exception {
                        return null;
                    }
                }
        );
    }
}
