package org.twister2.storage.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class ByteOutputFormat<T, V> implements OutputFormat<T, V> {

  @Override
  public RecordWriter<T, V> getRecordWriter(FileSystem fileSystem,
                                                        JobConf jobConf, String s,
                                                        Progressable progressable) throws IOException {
    return new EmptyRecordWriter<T, V>();
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

  }
}
