package org.twister2.storage.io;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import edu.iu.dsc.tws.api.comms.structs.Tuple;

import java.io.*;
import java.math.BigInteger;

/**
 * Writing the tweetid:time combination to a file
 */
public class TweetBufferedOutputWriter {

  /**
   * Keep track of the output stream, we need to close at the end
   */
  private FileOutputStream out;

  public TweetBufferedOutputWriter(String fileName) throws FileNotFoundException {
    this.out = new FileOutputStream(fileName);
  }

  public void write(BigInteger b, Long l) throws Exception {
    byte[] bigInts = b.toByteArray();
    byte[] timeBytes = Longs.toByteArray(l);
    int size = bigInts.length;
    try {
      this.out.write(Ints.toByteArray(size));
      this.out.write(bigInts);
      this.out.write(timeBytes);
    } catch (IOException e) {
      throw new Exception("Failed to write the tuple", e);
    }
  }

  public void write(String w) throws Exception {
    try {
      this.out.write(w.getBytes());
      this.out.write('\n');
    } catch (IOException e) {
      throw new Exception("Failed to write the tuple", e);
    }
  }

  public void close() {
    try {
      out.flush();
      out.close();
    } catch (IOException ignore) {
    }
  }
}
