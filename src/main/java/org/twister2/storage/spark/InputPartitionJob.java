package org.twister2.storage.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twister2.storage.tws.Context;

import java.math.BigInteger;
import java.util.Comparator;

public class InputPartitionJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<BigInteger, Long> input = sc.newAPIHadoopRDD(configuration, BinaryInput.class, BigInteger.class, Long.class);
    JavaPairRDD<BigInteger, Long> sorted = input.repartitionAndSortWithinPartitions(new HashPartitioner(Context.PARALLELISM), new Comparator<BigInteger>() {
      @Override
      public int compare(BigInteger o1, BigInteger o2) {
        return o1.compareTo(o2);
      }
    });
    sorted.saveAsHadoopFile("out", BigInteger.class, Long.class, TextOutputFormat.class);
    sc.stop();
  }
}
