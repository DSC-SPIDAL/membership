package org.twister2.storage.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.twister2.storage.tws.Context;

import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InputPartitionJob {
  private static final Logger LOG = Logger.getLogger(InputPartitionJob.class.getName());

  public static void main(String[] args) {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<Tuple2<BigInteger, Long>> s = env.createInput(new BinaryInput()).setParallelism(4);
    s.partitionByHash(0).sortPartition(0, Order.ASCENDING).writeAsCsv(Context.FILE_BASE + "/out");

    try {
      env.execute();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to execute", e);
    }
  }
}
