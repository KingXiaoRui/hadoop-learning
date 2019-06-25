package com.king.demo.sparksql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * @author DKing
 * @description
 * @date 2019/6/22
 */
public class SafeTypeAverageTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "examples/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        MyAverage myAverage = new MyAverage();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        ds.show();
        // +-------+------+
        // |   name|salary|
        // +-------+------+
        // |Michael|  3000|
        // |   Andy|  4500|
        // | Justin|  3500|
        // |  Berta|  4000|
        // +-------+------+

        result.show();
        // +--------------+
        // |average_salary|
        // +--------------+
        // |        3750.0|
        // +--------------+
    }


    public static class Employee implements Serializable {
        private String name;
        private long salary;

        public long getSalary() {
            return 0;
        }

        // Constructors, getters, setters...

    }

    public static class Average implements Serializable {
        private long sum;
        private long count;

        public Average(long l, long l1) {

        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
        // Constructors, getters, setters...

    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {
        // A zero value for this aggregation. Should satisfy the property that any b + zero = b
        @Override
        public Average zero() {
            return new Average(0L, 0L);
        }

        // Combine two values to produce a new value. For performance, the function may modify `buffer`
        // and return it instead of constructing a new object
        @Override
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        // Merge two intermediate values
        @Override
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        // Transform the output of the reduction
        @Override
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }

        // Specifies the Encoder for the intermediate value type
        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        // Specifies the Encoder for the final output value type
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}
