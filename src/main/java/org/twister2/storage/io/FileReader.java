package org.twister2.storage.io;

import edu.iu.dsc.tws.api.comms.structs.Tuple;

import java.io.IOException;
import java.math.BigInteger;

public interface FileReader {
  boolean reachedEnd() throws IOException;
  Tuple<BigInteger, Long> nextRecord();
}
