package org.twister2.storage.spark;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class EmptyRecordWriter<T, V> implements RecordWriter<T, V> {
  @Override
  public void write(T bi, V lo) throws IOException {
  }

  @Override
  public void close(Reporter reporter) throws IOException {
  }
}
