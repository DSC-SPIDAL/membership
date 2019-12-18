package org.twister2.storage.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.twister2.storage.tws.Context;

import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InputPartitionJob {
  private static final Logger LOG = Logger.getLogger(InputPartitionJob.class.getName());

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);

    DataSource<Tuple2<BigInteger, Long>> s = env.createInput(new BinaryInput()).setParallelism(params.getInt(Context.ARG_PARALLEL));
    s.partitionByHash(0).sortPartition(0, Order.ASCENDING).writeAsCsv(params.get(Context.ARG_FILE_PREFIX) + "/out",
        FileSystem.WriteMode.OVERWRITE);

    try {
      env.execute();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to execute", e);
    }
  }
}
