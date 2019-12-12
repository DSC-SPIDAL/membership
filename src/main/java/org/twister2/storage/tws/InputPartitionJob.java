package org.twister2.storage.tws;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.*;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import org.twister2.storage.io.TweetBufferedOutputWriter;
import org.twister2.storage.io.TwitterInputReader;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

public class InputPartitionJob implements IWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(InputPartitionJob.class.getName());

  private static int parallel = 40;

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(MembershipJob.class.getName())
        .setWorkerClass(InputPartitionJob.class)
        .addComputeResource(1, 48000, parallel)
        .setConfig(new JobConfig())
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    BatchTSetEnvironment batchEnv = BatchTSetEnvironment.initBatch(WorkerEnvironment.init(
        config, workerID, workerController, persistentVolume, volatileVolume));
    // first we are going to read the files and sort them
    SinkTSet<Iterator<Tuple<BigInteger, Iterator<Long>>>> sink1 = batchEnv.createSource(new SourceFunc<Tuple<BigInteger, Long>>() {
      TwitterInputReader reader;

      private TSetContext ctx;

      @Override
      public void prepare(TSetContext context) {
        this.ctx = context;
        reader = new TwitterInputReader("/data/input-" + context.getIndex());
      }

      @Override
      public boolean hasNext() {
        try {
          boolean b = reader.hasNext();
          if (!b) {
            LOG.info("Done reading from file - " + "/data/input-" + ctx.getIndex());
          }
          return b;
        } catch (Exception e) {
          throw new RuntimeException("Failed to read", e);
        }
      }

      @Override
      public Tuple<BigInteger, Long> next() {
        try {
          return reader.next();
        } catch (Exception e) {
          throw new RuntimeException("Failed to read next", e);
        }
      }
    }, parallel).mapToTuple(new MapFunc<Tuple<BigInteger, Long>, Tuple<BigInteger, Long>>() {
      @Override
      public Tuple<BigInteger, Long> map(Tuple<BigInteger, Long> input) {
        return input;
      }
    }).keyedGather().sink(new SinkFunc<Iterator<Tuple<BigInteger, Iterator<Long>>>>() {
      TweetBufferedOutputWriter writer;

      TSetContext context;

      @Override
      public void prepare(TSetContext context) {
        try {
          writer = new TweetBufferedOutputWriter("/data/outfile-" + context.getIndex());
          this.context = context;
        } catch (FileNotFoundException e) {
          throw new RuntimeException("Failed to write", e);
        }
      }

      @Override
      public boolean add(Iterator<Tuple<BigInteger, Iterator<Long>>> value) {
        LOG.info("Starting to save");
        while (value.hasNext()) {
          try {
            Tuple<BigInteger, Iterator<Long>> next = value.next();
            writer.write(next.getKey(), next.getValue().next());
          } catch (Exception e) {
            throw new RuntimeException("Failed to write", e);
          }
        }
        writer.close();
        return true;
      }
    });

    batchEnv.eval(sink1);
  }
}
