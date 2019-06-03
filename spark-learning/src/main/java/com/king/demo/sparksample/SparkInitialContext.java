package com.king.demo.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/5/29
 */
public class SparkInitialContext {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("wordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines1 = sc.parallelize(Arrays.asList("pandas", "i like pandas"));

        JavaRDD<String> lines2 = sc.textFile("README.md");

        JavaRDD<String> log = sc.textFile("log.txt");

        class ContainsError implements Function<String, Boolean> {
            private String query;

            public ContainsError(String query) {
                this.query = query;
            }

            @Override
            public Boolean call(String s) throws Exception {
                return s.contains(query);
            }
        }
        JavaRDD<String> errorLog = log.filter(new ContainsError("error"));

        JavaRDD<String> warningLog = log.filter(
                (Function<String, Boolean>) s -> s.contains("warning")
        );
        JavaRDD<String> badLog = errorLog.union(warningLog);

        System.out.println("Input had " + badLog.count() + " concerning lines");
        System.out.println("Here are 10 examples:");
        for (String line : badLog.take(10)) {
            System.out.println(line);
        }

        JavaRDD<String> pythonLines = lines2.filter(
                (Function<String, Boolean>) s -> s.contains("Python")
        );

        pythonLines.persist(StorageLevels.MEMORY_AND_DISK);

        String firstPythonLines = pythonLines.first();

        JavaRDD<String> words = lines2.flatMap(
                (FlatMapFunction<String, String>) s -> (Iterator<String>) Arrays.asList(s.split(" "))
        );

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        ).reduceByKey(
                (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2
        );

        counts.saveAsTextFile("README.md");
    }
}
