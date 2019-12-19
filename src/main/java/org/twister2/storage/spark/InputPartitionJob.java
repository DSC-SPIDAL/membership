package org.twister2.storage.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.math.BigInteger;

public class InputPartitionJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    String prefix = args[0] + "/csvData";
    int parallel = Integer.parseInt(args[1]);

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> input = sc.textFile(prefix, parallel);

    JavaPairRDD<BigInteger, Long>  source = input.mapToPair(new PairFunction<String, BigInteger, Long>() {
      @Override
      public Tuple2<BigInteger, Long> call(String s) throws Exception {
        String[] a = s.split(",");
        return new Tuple2<>(new BigInteger(a[0]), Long.parseLong(a[1]));
      }
    });
    source.sortByKey().saveAsTextFile(args[0] + "/sparkOut");
  }
}
