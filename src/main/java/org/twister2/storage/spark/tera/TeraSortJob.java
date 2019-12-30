package org.twister2.storage.spark.tera;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twister2.storage.spark.ByteOutputFormat;
import org.twister2.storage.tws.Context;

public class TeraSortJob {
  public static final String ARG_KEY_SIZE = "keySize";
  public static final String ARG_DATA_SIZE = "dataSize";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(ARG_KEY_SIZE, args[2]);
    configuration.set(ARG_DATA_SIZE, args[3]);


    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(input.partitions().size()), new ByteComparator());

    sorted.saveAsHadoopFile("out", byte[].class, byte[].class, ByteOutputFormat.class);
    sc.stop();
  }
}
