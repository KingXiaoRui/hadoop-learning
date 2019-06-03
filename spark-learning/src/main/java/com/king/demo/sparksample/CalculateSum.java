package com.king.demo.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author DKing
 * @description
 * @date 2019/6/2
 */
public class CalculateSum {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("calculateSum");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> counts = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        JavaDoubleRDD doubleRDD = counts.mapToDouble(
                (DoubleFunction<Integer>) integer -> integer * integer
        );
        System.out.println(doubleRDD.mean());


        Function2<AvgCount, Integer, AvgCount> addAndCount =
                (Function2<AvgCount, Integer, AvgCount>) (avgCount, integer) -> {
                    avgCount.total += integer;
                    avgCount.num += 1;
                    return avgCount;
                };
        Function2<AvgCount, AvgCount, AvgCount> combine =
                (Function2<AvgCount, AvgCount, AvgCount>) (avgCount, avgCount2) -> {
                    avgCount.total += avgCount2.total;
                    avgCount.num += avgCount2.num;
                    return avgCount;
                };

        AvgCount initial = new AvgCount(0, 0);
        AvgCount result = counts.aggregate(initial, addAndCount, combine);
        System.out.println(result.avg());
    }

    static class AvgCount implements Serializable {
        public int total;
        public int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total / (double) num;
        }
    }
}