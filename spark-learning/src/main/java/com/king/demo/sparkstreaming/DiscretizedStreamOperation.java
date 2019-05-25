package com.king.demo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * @author DKing
 * @description
 * @date 2019/5/15
 */
public class DiscretizedStreamOperation {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("1");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)).iterator()
        );

        JavaPairDStream<String, Integer> wordsCount = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        ).reduceByKey(
                (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2
        );
        wordsCount.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
