package org.twister2.storage.spark.tera;

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

public class TeraSortPartitioner extends Partitioner implements Serializable {
  private static final Logger LOG = Logger.getLogger(TeraSortPartitioner.class.getName());

  private int partitions;
  private int keysToOneTask;
  private List<Integer> destinationsList;

  public TeraSortPartitioner(int partitions) {
    this.partitions = partitions;
    Set<Integer> h = new HashSet<>();
    for (int j = 0; j < partitions; j++) {
      h.add(j);
    }
    prepare(h);
  }

  @Override
  public int getPartition(Object key) {
    LOG.info("Partition with key size: " + ((byte[]) key).length);
    return partition((byte[]) key);
  }

  private void prepare(Set<Integer> destinations) {
    int totalPossibilities = 256 * 256; //considering only most significant bytes of array
    this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
    this.destinationsList = new ArrayList<>(destinations);
    Collections.sort(this.destinationsList);
  }

  private int getIndex(byte[] array) {
    int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
    return key / keysToOneTask;
  }

  private int partition(byte[] data) {
    return this.destinationsList.get(this.getIndex(data));
  }

  @Override
  public int numPartitions() {
    return partitions;
  }
}
