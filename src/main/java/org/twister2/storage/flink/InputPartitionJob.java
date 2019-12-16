package org.twister2.storage.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigInteger;

public class InputPartitionJob {
  public static void main(String[] args) {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<Tuple2<BigInteger, Long>> s = env.createInput(new BinaryInput());
    s.partitionByHash(0).sortPartition(0, Order.ASCENDING).writeAsCsv("");
  }
}
