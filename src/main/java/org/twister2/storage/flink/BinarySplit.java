package org.twister2.storage.flink;

import org.apache.flink.core.io.InputSplit;

public class BinarySplit implements InputSplit {
  private int split;

  public BinarySplit(int split) {
    this.split = split;
  }

  @Override
  public int getSplitNumber() {
    return split;
  }
}
