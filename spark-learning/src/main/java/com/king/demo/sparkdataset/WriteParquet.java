package com.king.demo.sparkdataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author DKing
 * @description
 * @date 2019/5/25
 */
public class WriteParquet {
    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "true").getOrCreate();
        //给定一串表头
        String colstr = "id,name,age";
        //以,分割
        String[] cols = colstr.split(",");
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : cols) {
            //创建StructField，因不知其类型，默认转为字符型
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        //创建StructType
        StructType schema = DataTypes.createStructType(fields);
        List<Row> rows = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Row row = new GenericRow();
        }

        //创建只包含schema的Dataset
        Dataset<Row> data = sparkSession.createDataFrame(rows, schema);
        data.show();
        //存储为parquet格式
        data.write().format("parquet").parquet("/data.parquet");
    }
}
