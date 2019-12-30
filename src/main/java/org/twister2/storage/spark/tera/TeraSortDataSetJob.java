package org.twister2.storage.spark.tera;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.twister2.storage.spark.ByteOutputFormat;
import org.twister2.storage.tws.Context;
import scala.Tuple2;

public class TeraSortDataSetJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    configuration.set(Context.ARG_TUPLES, args[0]);
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(TeraSortJob.ARG_KEY_SIZE, args[2]);
    configuration.set(TeraSortJob.ARG_DATA_SIZE, args[3]);

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(input.partitions().size()), new ByteComparator());

    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate();

    StructType schema = new StructType(new StructField[]{
        new StructField("sentence", DataTypes.BinaryType, false, Metadata.empty()),
        new StructField("dependency", DataTypes.BinaryType, false, Metadata.empty())
    });

    Dataset<Row> row = spark.createDataset(JavaPairRDD.toRDD(input), Encoders.tuple(Encoders.BINARY(),Encoders.BINARY())).toDF();
    sorted.saveAsHadoopFile("out", byte[].class, byte[].class, ByteOutputFormat.class);
    sc.stop();
  }

  public static final Function<Tuple2<byte[], byte[]>, Row> mappingFunc = (tuple) -> {
    return RowFactory.create(tuple._1(),tuple._2());
  };
}
