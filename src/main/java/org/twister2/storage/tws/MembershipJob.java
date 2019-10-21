package org.twister2.storage.tws;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.resource.*;
import edu.iu.dsc.tws.api.tset.RecordCollector;
import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import org.twister2.storage.io.TweetBufferedOutputWriter;
import org.twister2.storage.io.TwitterInputReader;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Finding the membership
 */
public class MembershipJob implements IWorker, Serializable {
  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(MembershipJob.class.getName())
        .setWorkerClass(MembershipJob.class)
        .addComputeResource(1, 2048, 4)
        .setConfig(new JobConfig())
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    BatchTSetEnvironment batchEnv = BatchTSetEnvironment.initBatch(WorkerEnvironment.init(
        config, workerID, workerController, persistentVolume, volatileVolume));
    // first we are going to read the files and sort them
    batchEnv.createSource(new SourceFunc<Tuple<BigInteger, Long>>() {
      TwitterInputReader reader;
      @Override
      public void prepare(TSetContext context) {
        reader = new TwitterInputReader("/tmp/input" + context.getIndex());
      }

      @Override
      public boolean hasNext() {
        try {
          return reader.hasNext();
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
    }, 4).mapToTuple(new MapFunc<Tuple<BigInteger, Long>, Tuple<BigInteger, Long>>() {
      @Override
      public Tuple<BigInteger, Long> map(Tuple<BigInteger, Long> input) {
        return input;
      }
    }).keyedGather().sink(new SinkFunc<Iterator<Tuple<BigInteger, Iterator<Long>>>>() {
      TweetBufferedOutputWriter writer;
      @Override
      public void prepare(TSetContext context) {
        try {
          writer = new TweetBufferedOutputWriter("/tmp/outfile-" + context.getIndex());
        } catch (FileNotFoundException e) {
          throw new RuntimeException("Failed to write", e);
        }
      }

      @Override
      public boolean add(Iterator<Tuple<BigInteger, Iterator<Long>>> value) {
        while(value.hasNext()) {
          try {
            writer.write(value.next().getKey(), value.next().getValue().next());
          } catch (Exception e) {
            throw new RuntimeException("Failed to write", e);
          }
        }
        writer.close();
        return true;
      }
    });

    // now lets read the second input file
    Storable<Tuple<BigInteger, Long>> secondInput = batchEnv.createSource(new SourceFunc<Tuple<BigInteger, Long>>() {
      TwitterInputReader reader;

      @Override
      public void prepare(TSetContext context) {
        reader = new TwitterInputReader("/tmp/second-input" + context.getIndex());
      }

      @Override
      public boolean hasNext() {
        try {
          return reader.hasNext();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Tuple<BigInteger, Long> next() {
        try {
          return reader.next();
        } catch (Exception e) {
          throw new RuntimeException();
        }
      }
    }, 4).cache();

    BatchTSet<Tuple<BigInteger, Long>> savedInput = batchEnv.createSource(new SourceFunc<Tuple<BigInteger, Long>>() {
      TwitterInputReader reader;
      @Override
      public void prepare(TSetContext context) {
        reader = new TwitterInputReader("/tmp/outfile-" + context.getIndex());
      }

      @Override
      public boolean hasNext() {
        try {
          return reader.hasNext();
        } catch (Exception e) {
          throw new RuntimeException("Failed to read", e);
        }
      }

      @Override
      public Tuple<BigInteger, Long> next() {
        try {
          return reader.next();
        } catch (Exception e) {
          throw new RuntimeException("Failed to read", e);
        }
      }
    }, 4);


    BatchTSet<String> b = savedInput.direct().flatmap(new FlatMapFunc<String, Tuple<BigInteger, Long>>() {
      Map<BigInteger, Long> inputMap = new HashMap<>();

      @Override
      public void prepare(TSetContext context) {
        DataPartition a = context.getInput("input");
        DataPartitionConsumer<Tuple<BigInteger, Long>> consumer = a.getConsumer();
        while (consumer.hasNext()) {
          Tuple<BigInteger, Long> bigIntegerLongTuple = consumer.next();
          inputMap.put(bigIntegerLongTuple.getKey(), bigIntegerLongTuple.getValue());
        }
      }

      @Override
      public void flatMap(Tuple<BigInteger, Long> input, RecordCollector<String> collector) {
        if (inputMap.containsKey(input.getKey())) {
          try {
            collector.collect(input.getKey().toString() + "," + input.getValue());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    });
    b.addInput("input", secondInput);

    b.direct().sink(new SinkFunc<Iterator<String>>() {
      TweetBufferedOutputWriter writer;

      @Override
      public void prepare(TSetContext context) {
        try {
          writer = new TweetBufferedOutputWriter("/tmp/final-" + context.getIndex());
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean add(Iterator<String> value) {
        while (value.hasNext()) {
          String input = value.next();
          try {
            writer.write(input);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
    });
  }
}
