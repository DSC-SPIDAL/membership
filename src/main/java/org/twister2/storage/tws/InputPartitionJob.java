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
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import org.twister2.storage.io.StreamInputReader;
import org.twister2.storage.io.TweetBufferedOutputWriter;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

public class InputPartitionJob implements IWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(InputPartitionJob.class.getName());

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    JobConfig jobConfig = new JobConfig();

    String filePrefix = args[0];
    int parallel = Integer.parseInt(args[1]);
    int memory = Integer.parseInt(args[2]);

    jobConfig.put(Context.ARG_FILE_PREFIX, filePrefix);
    jobConfig.put(Context.ARG_PARALLEL, parallel);
    jobConfig.put(Context.ARG_MEMORY, memory);

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(MembershipJob.class.getName())
        .setWorkerClass(InputPartitionJob.class)
        .addComputeResource(1, memory, parallel)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    BatchTSetEnvironment batchEnv = BatchTSetEnvironment.initBatch(WorkerEnvironment.init(
        config, workerID, workerController, persistentVolume, volatileVolume));
    int parallel = config.getIntegerValue(Context.ARG_PARALLEL);
    // first we are going to read the files and sort them
    SinkTSet<Iterator<Tuple<BigInteger, Iterator<Long>>>> sink1 = batchEnv.createKeyedSource(new SourceFunc<Tuple<BigInteger, Long>>() {
      StreamInputReader reader;

      private TSetContext ctx;

      private String prefix;

      @Override
      public void prepare(TSetContext context) {
        this.ctx = context;
        prefix = context.getConfig().getStringValue(Context.ARG_FILE_PREFIX);
        reader = new StreamInputReader(prefix + "/data/input-" + context.getIndex(), context.getConfig());
      }

      @Override
      public boolean hasNext() {
        try {
          boolean b = reader.reachedEnd();
          if (b) {
            LOG.info("Done reading from file - " + prefix + "/data/input-" + ctx.getIndex());
          }
          return !b;
        } catch (Exception e) {
          throw new RuntimeException("Failed to read", e);
        }
      }

      @Override
      public Tuple<BigInteger, Long> next() {
        try {
          return reader.nextRecord();
        } catch (Exception e) {
          throw new RuntimeException("Failed to read next", e);
        }
      }
    }, parallel).keyedGather(new HashingPartitioner<>(), new Comparator<BigInteger>() {
      @Override
      public int compare(BigInteger o1, BigInteger o2) {
        return o1.compareTo(o2);
      }
    }).useDisk().sink(new SinkFunc<Iterator<Tuple<BigInteger, Iterator<Long>>>>() {
      TweetBufferedOutputWriter writer;

      TSetContext context;

      @Override
      public void prepare(TSetContext context) {
        try {
          String prefix = context.getConfig().getStringValue(Context.ARG_FILE_PREFIX);
          writer = new TweetBufferedOutputWriter(prefix + "/data/outfile-" + context.getIndex(), context.getConfig());
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
