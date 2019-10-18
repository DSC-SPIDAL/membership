package org.twister2.storage.io;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.shuffle.MemoryMapUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TweetOutputWriter {
  private static final Logger LOG = Logger.getLogger(TweetOutputWriter.class.getName());

  public void writeValues(List<Tuple<BigInteger, Long>> records, String outFileName) throws Exception {
    try {
      // first serialize keys
      long totalSize = 0;
      List<byte[]> bigIntBytes;

      bigIntBytes = new ArrayList<>(records.size()); //only initialize if required
      for (Tuple<BigInteger, Long> record : records) {
        byte[] data = record.getKey().toByteArray();
        totalSize += data.length; // data + length of key
        bigIntBytes.add(data);
      }

      // we need to write the data lengths and key lengths
      totalSize += (Integer.BYTES + Long.BYTES) * records.size();

      Files.createDirectories(Paths.get(outFileName).getParent());
      RandomAccessFile randomAccessFile = new RandomAccessFile(outFileName, "rw");
      FileChannel rwChannel = randomAccessFile.getChannel();
      MappedByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
      int i = 0;

      for (Tuple<BigInteger, Long> keyValue : records) {
        byte[] bigInt = bigIntBytes.get(i);
        long date = keyValue.getValue();
        os.putInt(bigInt.length);
        os.put(bigInt);
        os.putLong(date);
        i++;
      }

      try {
        MemoryMapUtils.unMapBuffer(os);
      } catch (Exception e) {
        //ignore
        LOG.warning("Couldn't unmap a byte buffer manually");
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new Exception("Failed to write to disc", e);
    }
  }
}
