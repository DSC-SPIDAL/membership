package org.twister2.storage.spark.tera;

import org.apache.hadoop.mapreduce.*;
import org.twister2.storage.tws.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ByteInputFormat extends InputFormat<byte[], byte[]> {
  private static final Logger LOG = Logger.getLogger(ByteInputFormat.class.getName());
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    int parallel = jobContext.getConfiguration().getInt(Context.ARG_PARALLEL, 16);
    int keySize = jobContext.getConfiguration().getInt(TeraSortJob.ARG_KEY_SIZE, 10);
    int dataSize = jobContext.getConfiguration().getInt(TeraSortJob.ARG_DATA_SIZE, 90);
    int elements = jobContext.getConfiguration().getInt(Context.ARG_TUPLES, 10000);

    LOG.info(String.format("Format configuration parallel %d, key %d, data %d, tuples %d",
        parallel, keySize, dataSize, elements));

    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < parallel; i++) {
      String node = "v-0";
      int index = i % 16;
      if (index >= 10) {
        node += index;
      } else {
        node += "0" + index;
      }
      ByteInputSplit e = new ByteInputSplit(elements, keySize, dataSize, node);
      e.setNode(node);
      splits.add(e);
    }
    return splits;
  }

  @Override
  public RecordReader<byte[], byte[]> createRecordReader(InputSplit inputSplit,
                                                         TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (inputSplit instanceof ByteInputSplit) {
      return new ByteRecordReader();
    } else {
      throw new RuntimeException("Un-expected split");
    }
  }
}
