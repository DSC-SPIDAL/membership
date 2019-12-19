package org.twister2.storage.io;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

import java.io.*;
import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVStreamInputReader implements FileReader {
  private static final Logger LOG = Logger.getLogger(StreamInputReader.class.getName());

  private BufferedReader in;

  private String currentSize;

  private boolean end = false;

  private int count;

  public CSVStreamInputReader(String fileName, Config config) {
    try {
      FileSystem fs = FileSystemUtils.get(new Path(fileName).toUri(), config);
      this.in = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create file system - " + fileName);
    }
  }

  public boolean reachedEnd() throws IOException {
    try {
      currentSize = in.readLine();
      if (currentSize == null) {
        end = true;
      }
      count++;
    } catch (EOFException e) {
      end = true;
      LOG.info("End reached - read tuples - " + count);
    }
    return end;
  }

  public Tuple<BigInteger, Long> nextRecord() {
    String[] a = currentSize.split(",");
    return new Tuple<>(new BigInteger(a[0]), Long.parseLong(a[1]));
  }
}
