package com.king.demo.sparksample;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author DKing
 * @description
 * @date 2019/6/16
 */
public class JsonProcessor {
    public static void main(String[] args) {
        class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
            @Override
            public Iterator<String> call(Iterator<Person> personIterator) throws Exception {
                ArrayList<String> text = new ArrayList<>();
                ObjectMapper mapper = new ObjectMapper();
                while (personIterator.hasNext()) {
                    Person person = personIterator.next();
                    text.add(mapper.writeValueAsString(person));
                }
                return text.iterator();
            }
        }
        class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
            @Override
            public Iterator<Person> call(Iterator<String> stringIterator) throws Exception {
                ArrayList<Person> people = new ArrayList<>();
                ObjectMapper mapper = new ObjectMapper();
                while (stringIterator.hasNext()) {
                    String line = stringIterator.next();
                    try {
                        people.add(mapper.readValue(line, Person.class));
                    } catch (Exception e) {
                        // 跳过失败的数据
                    }
                }
                return people.iterator();
            }
        }
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("file.json");

        JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(
                new LikesPandas());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        String outfile = "/opt/getPandasLiker";
        formatted.saveAsTextFile(outfile);
    }
}
