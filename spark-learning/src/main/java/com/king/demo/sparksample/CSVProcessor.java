package com.king.demo.sparksample;

import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/6/19
 */
public class CSVProcessor {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputFile = "";
        JavaRDD<String> csvFile1 = sc.textFile(inputFile);
        JavaRDD<String[]> csvData = csvFile1.map(new ParseLine());

        JavaPairRDD<String, String> csvData2 = sc.wholeTextFiles(inputFile);
        JavaRDD<String[]> keyedRDD = csvData2.flatMap(new ParseLine2());
    }

    public static class ParseLine2 implements FlatMapFunction<Tuple2<String, String>, String[]> {

        @Override
        public Iterator<String[]> call(Tuple2<String, String> stringStringTuple2) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(stringStringTuple2._2()));
            return reader.readAll().iterator();
        }
    }

    public static class ParseLine implements Function<String, String[]> {
        @Override
        public String[] call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            return reader.readNext();
        }
    }
}
