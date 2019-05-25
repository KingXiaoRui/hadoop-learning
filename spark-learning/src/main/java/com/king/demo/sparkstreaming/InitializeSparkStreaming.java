package com.king.demo.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author DKing
 * @description
 * @date 2019/5/15
 */
public class InitializeSparkStreaming {

    public void fun1() {

        /**
         * 要连接的主URL，例如“本地”在本地运行一个线程，“local [4]”在本地运行4个核心
         * ，或“spark：// master：7077”在Spark独立集群上运行。
         */
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("1");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
    }

    public void fun2() {
        SparkConf conf = new SparkConf().setAppName("2").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jss = new JavaStreamingContext(sc, Durations.seconds(1));

    }
}
