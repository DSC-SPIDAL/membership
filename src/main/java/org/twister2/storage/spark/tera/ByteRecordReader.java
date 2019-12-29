package org.twister2.storage.spark.tera;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

public class ByteRecordReader extends RecordReader<byte[], byte[]> {
  private static final Logger LOG = Logger.getLogger(ByteRecordReader.class.getName());

  private int numRecords;
  private int currentRead = 0;
  private Random random;
  private int keySize;
  private int dataSize;

  public ByteRecordReader() {
    random = new Random(System.nanoTime());
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (inputSplit instanceof ByteInputSplit) {
      ByteInputSplit split = (ByteInputSplit) inputSplit;
      numRecords = split.getElements();
      keySize = split.getKeySize();
      dataSize = split.getDataSize();
      LOG.info(String.format("Records %d, key %d, data %d", numRecords, keySize, dataSize));
    } else {
      throw new IOException("Not a ByteInputSplit");
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return currentRead++ < numRecords;
  }

  @Override
  public byte[] getCurrentKey() throws IOException, InterruptedException {
    byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

  @Override
  public byte[] getCurrentValue() throws IOException, InterruptedException {
    byte[] key = new byte[dataSize];
    random.nextBytes(key);
    return key;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentRead / numRecords;
  }

  @Override
  public void close() throws IOException {

  }
}
