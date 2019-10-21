package org.twister2.storage.io;

import com.google.common.primitives.Longs;

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
    try {
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
    for (int pic = 0; pic < 4; pic++) {
      PrintWriter pw = new PrintWriter(new FileWriter("input-" + pic));
      for (int i = 0; i < 1000; i++) {
        pw.println(i + "," + pic);
      }
      pw.close();
    }
  }
}
