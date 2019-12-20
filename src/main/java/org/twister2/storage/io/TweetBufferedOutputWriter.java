package org.twister2.storage.io;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

import java.io.*;
import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writing the tweetid:time combination to a file
 */
public class TweetBufferedOutputWriter {
  private static final Logger LOG = Logger.getLogger(TweetBufferedOutputWriter.class.getName());

  /**
   * Keep track of the output stream, we need to close at the end
   */
  private FSDataOutputStream out;

  public TweetBufferedOutputWriter(String fileName, Config config) throws FileNotFoundException {
    try {
      FileSystem fs = FileSystemUtils.get(new Path(fileName).toUri(), config);
      this.out = fs.create(new Path(fileName));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create file system - " + fileName);
    }
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

  public void writeWithoutEnd(String w) throws Exception {
    try {
      this.out.write(w.getBytes());
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
