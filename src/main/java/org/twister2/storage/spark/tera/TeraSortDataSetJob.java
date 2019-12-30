package org.twister2.storage.spark.tera;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.twister2.storage.tws.Context;

public class TeraSortDataSetJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(TeraSortJob.ARG_KEY_SIZE, args[2]);
    configuration.set(TeraSortJob.ARG_DATA_SIZE, args[3]);

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);

    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate();

    Dataset<Row> row = spark.createDataset(JavaPairRDD.toRDD(input), Encoders.tuple(Encoders.BINARY(),Encoders.BINARY())).toDF("key", "value");
    row.repartition(input.getNumPartitions(), row.col("key")).sortWithinPartitions(row.col("key"));
    row.write().csv(args[4]);
  }
}
