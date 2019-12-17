package org.twister2.storage.spark;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class BinaryInput extends InputFormat<BigInteger, Long> {
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    return null;
  }

  @Override
  public RecordReader<BigInteger, Long> createRecordReader(InputSplit inputSplit,
                                                         TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return null;
  }
}
