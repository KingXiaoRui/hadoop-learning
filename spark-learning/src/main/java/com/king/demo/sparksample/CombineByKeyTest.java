package com.king.demo.sparksample;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Map;


/**
 * @author DKing
 * @description
 * @date 2019/6/4
 */
public class CombineByKeyTest {
    public static void main(String[] args) {
        Function<Integer, AvgCount> createAcc = integer
                -> new AvgCount(integer, 1);
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
        JavaPairRDD nums = null;
        JavaPairRDD<String, AvgCount> avgCounts =
                nums.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }

    private static class AvgCount implements Serializable {
        private int total;
        private int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public float avg() {
            return this.total / this.num;
        }
    }
}
