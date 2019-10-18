package org.twister2.storage.io;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Read a tweetid:time
 */
public class TwitterInputReader {
  /**
   * Name of the file to read
   */
  private String fileName;

  private ByteBuffer inputBuffer;

  private long totalRead = 0;

  private  FileChannel rwChannel;

  public TwitterInputReader(String file) {
    this.fileName = file;

    String outFileName = Paths.get(fileName).toString();

    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, rwChannel.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  Tuple<BigInteger, Long> next() {
    String outFileName = Paths.get(fileName).toString();
    try {
      List<Tuple> keyValues = new ArrayList<>();
      // lets read the key values
      int count = 0;
      while (totalRead < rwChannel.size()) {

      }
      rwChannel.force(true);
      rwChannel.close();
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  boolean hasNext() throws Exception {
    try {
      return totalRead < rwChannel.size();
    } catch (IOException e) {
      throw new Exception("Failed to read file", e);
    }
  }
}
