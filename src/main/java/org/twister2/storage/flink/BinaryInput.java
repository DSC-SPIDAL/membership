package org.twister2.storage.flink;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BinaryInput implements InputFormat<Tuple2<BigInteger, Long>, BinarySplit> {
  private static final Logger LOG = Logger.getLogger(BinarySplit.class.getName());

  private String filePrefix;

  private DataInputStream stream;

  private boolean end = false;

  private int count = 0;

  public BinaryInput(String filePrefix) {
    this.filePrefix = filePrefix + "/data/input-";
  }

  @Override
  public void configure(Configuration configuration) {
    if (filePrefix == null) {
      LOG.log(Level.SEVERE, "Prefix is NULL");
      throw new RuntimeException("Prefix is NULL");
    }
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
    return null;
  }

  @Override
  public BinarySplit[] createInputSplits(int num) throws IOException {
    BinarySplit[] split = new BinarySplit[num];
    for (int i = 0; i < num; i++) {
      split[i] = new BinarySplit(i);
    }
    return split;
  }

  public class FixedInputAssigner implements InputSplitAssigner {
    private BinarySplit[] splits;

    private HashMap<Integer, BinarySplit> consumed = new HashMap<>();

    public FixedInputAssigner(BinarySplit[] splits) {
      for (BinarySplit split : splits) {
        consumed.put(split.getSplitNumber(), split);
      }

    }

    @Override
    public InputSplit getNextInputSplit(String s, int i) {
      if (consumed.containsKey(i)) {
        return consumed.remove(i);
      }
      return null;
    }

    @Override
    public void returnInputSplit(List<InputSplit> list, int i) {
      throw new RuntimeException("Failed to return");
    }
  }


  @Override
  public InputSplitAssigner getInputSplitAssigner(BinarySplit[] binarySplits) {
    return new FixedInputAssigner(binarySplits);
  }

  @Override
  public void open(BinarySplit binarySplit) throws IOException {
    try {
      String file = filePrefix + binarySplit.getSplitNumber();
      FileSystem fs = FileSystem.get(new URI(file));
      stream = new DataInputStream(fs.open(new Path(file)));
      end = false;
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  private int currentSize = 0;

  @Override
  public boolean reachedEnd() throws IOException {
    try {
      currentSize = stream.readInt();
    } catch (EOFException e) {
      end = true;
      LOG.info("End reached - read tuples - " + count);
    }
    LOG.info("Return - " + end);
    return end;
  }


  @Override
  public Tuple2<BigInteger, Long> nextRecord(Tuple2<BigInteger, Long> o) throws IOException {
    try {
      byte[] intBuffer = new byte[currentSize];
      int read = read(intBuffer, 0, currentSize);
      if (read != currentSize) {
        throw new RuntimeException("Invalid file: read" + count);
      }

      BigInteger tweetId = new BigInteger(intBuffer);
      long time = stream.readLong();
      count++;
      return new Tuple2<>(tweetId, time);
    } catch (EOFException e) {
      end = true;
      LOG.log(Level.SEVERE, "End reached - read tuples - " + count, e);
      throw new RuntimeException("We cannot reach end here", e);
    }
  }

  private int read(byte[] b, int off, int len) throws IOException {
    int totalRead = 0;
    for (int remainingLength = len, offset = off; remainingLength > 0;) {
      int read = this.stream.read(b, offset, remainingLength);
      if (read < 0) {
        return read;
      }
      totalRead += read;
      offset += read;
      remainingLength -= read;
    }
    return totalRead;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
