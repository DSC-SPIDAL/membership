package org.twister2.storage.spark;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.math.BigInteger;

public class EmptyRecordWriter implements RecordWriter<BigInteger, Long> {
  @Override
  public void write(BigInteger bi, Long lo) throws IOException {
  }

  @Override
  public void close(Reporter reporter) throws IOException {
  }
}
