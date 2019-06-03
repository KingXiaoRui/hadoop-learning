package com.king.demo.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


/**
 * @author DKing
 * @description
 * @date 2019/6/3
 */
public class PairRDDTransactionSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TransactionSample")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaPairRDD<String, String> pairRDD = lines.mapToPair(
                (PairFunction<String, String, String>) s -> new Tuple2<>(s.split(" ")[0], s)
        );

        Function<Tuple2<String, String>, Boolean> longWordFilter =
                stringStringTuple2 -> (stringStringTuple2._2().length() < 20);

        JavaPairRDD<String, String> result = pairRDD.filter(longWordFilter);
    }
}
