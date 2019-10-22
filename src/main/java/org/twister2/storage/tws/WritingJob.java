package org.twister2.storage.tws;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.*;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import org.twister2.storage.io.TweetBufferedOutputWriter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.logging.Logger;

public class WritingJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(WritingJob.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    try {
      TweetBufferedOutputWriter outputWriter = new TweetBufferedOutputWriter("/tmp/input-" + workerID);
      BigInteger start = new BigInteger("100000000000000000").multiply(new BigInteger("" + (workerID + 1)));
      // now write 1000,000
      for (int i = 0; i < 100000000; i++) {
        BigInteger bi = start.add(new BigInteger(i + ""));
        if (i % 1000000 == 0) {
          LOG.info("Wrote elements: " + i);
        }
        outputWriter.write(bi, (long) workerID);
      }
      outputWriter.close();

      TweetBufferedOutputWriter outputWriter2 = new TweetBufferedOutputWriter("/tmp/second-input-" + workerID);
      BigInteger start2 = new BigInteger("100000000000000000").multiply(new BigInteger("" + (workerID + 1)));
      // now write 1000,000
      for (int i = 0; i < 1000000; i++) {
        BigInteger bi = start2.add(new BigInteger(i + ""));
        outputWriter2.write(bi, (long) workerID);
      }
      outputWriter2.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(WritingJob.class.getName())
        .setWorkerClass(WritingJob.class)
        .addComputeResource(1, 24000, 40)
        .setConfig(new JobConfig())
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
