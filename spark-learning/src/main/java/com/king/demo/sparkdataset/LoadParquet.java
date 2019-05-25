package com.king.demo.sparkdataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author DKing
 * @description
 * @date 2019/5/25
 */
public class LoadParquet {
    private static final String path = "E:\\hive-parquet\\test.parquet";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true").getOrCreate();

        Dataset<Row> data = sparkSession.read().parquet(path);
        data.show();
    }
}
