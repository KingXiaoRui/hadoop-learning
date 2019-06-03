package com.king.demo.sparksample;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * @author DKing
 * @description
 * @date 2019/6/2
 */
public class CalcuDouble {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("wordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(
                (Function<Integer, Integer>) integer -> integer * integer
        );
        System.out.println(StringUtils.join(result.collect(), ","));
    }
}
