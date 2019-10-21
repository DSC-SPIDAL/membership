package org.twister2.storage.io;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import edu.iu.dsc.tws.api.comms.structs.Tuple;

import java.io.*;
import java.math.BigInteger;

public class TweetBufferedOutputWriter {
  private String fileName;

  private FileOutputStream out;

  public TweetBufferedOutputWriter(String fileName) throws FileNotFoundException {
    this.fileName = fileName;
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

  public static void main(String[] args) throws IOException {
    try {
      TweetBufferedOutputWriter outputWriter = new TweetBufferedOutputWriter("/tmp/input-" + 0);
      BigInteger start = new BigInteger("100000000000000000").multiply(new BigInteger("" + 0));
      // now write 1000,000
      for (int i = 0; i < 10; i++) {
        BigInteger bi = start.add(new BigInteger(i + ""));
        outputWriter.write(bi, 0l);
      }
      outputWriter.close();


      TwitterInputReader reader = new TwitterInputReader("/tmp/input-0");
      while (reader.hasNext()) {
        Tuple<BigInteger, Long> t = reader.next();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
