/*
  Copyright 2009 RapLeaf

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public abstract class Relevance {
  public static boolean TEST_MODE = false;

  private static Logger LOG = Logger.getLogger(Relevance.class);

  private static String ID = "__id";
  private static String RELEVANT_OBJECT = "__relevantObject";

  public static abstract class RelevanceFunction implements Serializable {
    public List<Object> getMatches(TupleEntry tuple) {
      List<Object> ret = extractPotentialMatches(tuple);
      if (ret.size() > 1 && !canHaveMultipleMatches())
        throw new RuntimeException(
            "OneOrZero relevance function but more than one key matched "
                + ret.toString());
      return ret;
    }

    protected abstract List<Object> extractPotentialMatches(TupleEntry tuple);

    public boolean canHaveMultipleMatches() {
      return true;
    }
  }

  /**
   * Does exact query without using a bloom filter Eventually, when we are
   * absorbing data at a very very high rate, we will need to switch to use this
   * and not bloom filter approach (because false positives will be too high)
   */
  public void batch_query(Tap source, Tap output, Fields wantedFields,
      RelevanceFunction func, Tap keysTap, String keyField) throws IOException {
    batch_query(source, output, wantedFields, func, keysTap, keyField, false,
        -1, -1, true);
  }

  public void batch_query(Tap source, Tap output, Fields wantedFields,
      RelevanceFunction func, Tap keysTap, String keyField, int bloom_bits,
      int bloom_hashes, boolean exact) throws IOException {
    batch_query(source, output, wantedFields, func, keysTap, keyField, true,
        bloom_bits, bloom_hashes, exact);
  }

  /**
   * Exact relevance is slower, non-exact relevance will have false positives
   */
  protected void batch_query(Tap source, Tap output, Fields wantedFields,
      RelevanceFunction func, Tap keysTap, String keyField, boolean useBloom,
      int bloom_bits, int bloom_hashes, boolean exact) throws IOException {
    if (!useBloom && !exact)
      throw new IllegalArgumentException(
          "Must either use bloom filter or be exact, or both!");

    FileSystem fs = FileSystem.get(new Configuration());
    Pipe finalPipe = new Pipe("data");
    finalPipe = new Each(finalPipe, wantedFields, new Identity());

    Map<String, Tap> sources = new HashMap<String, Tap>();

    sources.put("data", source);
    Map properties = new HashMap();

    String bloomFilterPath = "/tmp/" + UUID.randomUUID().toString()
        + ".bloomfilter";
    if (useBloom) {
      String jobId = UUID.randomUUID().toString();

      LOG.info("Creating bloom filter");
      writeOutBloomFilter(keysTap, keyField, fs, bloomFilterPath, bloom_bits,
          bloom_hashes);
      properties.put("mapred.job.reuse.jvm.num.tasks", -1);
      if (!TEST_MODE) {
        properties.put("mapred.cache.files", "hdfs://" + bloomFilterPath);
      } else {
        properties.put("batch_query.relevance.file", bloomFilterPath);
      }
      LOG.info("Done creating bloom filter");

      finalPipe = new Each(finalPipe, wantedFields, getRelevanceFilter(func,
          jobId));

    }

    if (exact) {
      sources.put("relevant", keysTap);

      Pipe relevantRecords = new Pipe("relevant");
      relevantRecords = new Each(relevantRecords, new Fields(keyField),
          new Identity());
      
      finalPipe = new Each(finalPipe, wantedFields, getExactFilter(func),
          Fields.join(wantedFields, new Fields(ID, RELEVANT_OBJECT)));

      finalPipe = new CoGroup(finalPipe, new Fields(RELEVANT_OBJECT),
          relevantRecords, new Fields(keyField), Fields.join(wantedFields,
              new Fields(ID, RELEVANT_OBJECT), new Fields("__ignored")));

      finalPipe = new Each(finalPipe,
          Fields.join(wantedFields, new Fields(ID)), new Identity());

      if (func.canHaveMultipleMatches()) {
        finalPipe = new Distinct(finalPipe, new Fields(ID));
      }
      finalPipe = new Each(finalPipe, wantedFields, new Identity());
    }

    Flow flow = new FlowConnector(properties).connect("Relevance: "
        + func.getClass().getSimpleName(), sources, output, finalPipe);
    flow.complete();

    if (useBloom)
      fs.delete(new Path(bloomFilterPath), false);
  }

  protected static abstract class ExtractionFunction extends BaseOperation
      implements Function {
    private RelevanceFunction _func;

    public ExtractionFunction(RelevanceFunction func, String out_field) {
      super(new Fields(out_field));
      _func = func;
    }

    public void operate(FlowProcess flowProcess, FunctionCall call) {
      List<Object> rel = _func.getMatches(call.getArguments());
      for (Object obj : rel) {
        WritableComparable wc = toWritableComparable(obj);
        call.getOutputCollector().add(new Tuple(wc));
      }
    }

    protected abstract WritableComparable toWritableComparable(Object relevant);

  }

  protected static abstract class PrepareExactFilter extends BaseOperation
      implements Function {
    private RelevanceFunction _func;
    private int _counter;

    public PrepareExactFilter(RelevanceFunction func) {
      super(new Fields(ID, RELEVANT_OBJECT));
      _func = func;
      _counter = 0;
    }

    public void operate(FlowProcess flowProcess, FunctionCall call) {
      List<Object> rel = _func.getMatches(call.getArguments());

      // generate a unique id for this record in the given job
      // {taskId}-{counter}. The "-" is very important.
      // note that this ID is generated deterministically, so we don't run into
      // any random number problems
      int taskId = ((HadoopFlowProcess) flowProcess).getCurrentTaskNum();
      _counter++;
      String uuid_for_task = "" + taskId + "-" + _counter;
      for (Object obj : rel) {
        WritableComparable wc = toWritableComparable(obj);
        call.getOutputCollector().add(new Tuple(uuid_for_task, wc));
      }
    }

    protected abstract WritableComparable toWritableComparable(Object relevant);

  }

  protected abstract void writeOutBloomFilter(Tap keysTap, String keyField,
      FileSystem fs, String path, int bloom_bits, int bloom_hashes)
      throws IOException;

  protected abstract ExtractionFunction getExtractionFunction(
      RelevanceFunction func, String out_field);

  protected abstract PrepareExactFilter getExactFilter(RelevanceFunction func);

  protected abstract RelevanceFilter getRelevanceFilter(RelevanceFunction func,
      String job_id);

  protected static abstract class RelevanceFilter extends BaseOperation
      implements Filter {
    protected static Object _filter = null;
    protected static String _filter_job_id = null;

    // the job id stuff is so this stuff works on both cluster and during tests.
    // In tests the static objects don't get
    // cleared between jobs
    private RelevanceFunction _func;
    private String _job_id;

    public RelevanceFilter(RelevanceFunction func, String job_id) {
      super();
      _func = func;
      _job_id = job_id;
    }

    public boolean isRemove(FlowProcess process, FilterCall call) {
      if (_filter == null || !_filter_job_id.equals(_job_id)) {
        try {
          LOG.info("Loading bloom filter");

          if (!TEST_MODE) {
            Path[] files = DistributedCache
                .getLocalCacheFiles(((HadoopFlowProcess) process).getJobConf());
            if (files.length != 1)
              throw new RuntimeException(
                  "Expected one path in the Distributed cache: there were "
                      + files.length);
            _filter = loadFilter(FileSystem.getLocal(new Configuration()),
                files[0].toString());
          } else {
            _filter = loadFilter(FileSystem.get(new Configuration()),
                ((HadoopFlowProcess) process).getJobConf().get(
                    "batch_query.relevance.file"));
          }
          _filter_job_id = _job_id;

          LOG.info("Done loading bloom filter");
        } catch (IOException ioe) {
          throw new RuntimeException("Error loading bloom filter", ioe);
        }
      }
      List<Object> potential = _func.extractPotentialMatches(call
          .getArguments());
      for (Object obj : potential) {
        if (mayContain(_filter, obj))
          return false;
      }
      return true;
    }

    protected abstract Object loadFilter(FileSystem fs, String filterFile)
        throws IOException;

    protected abstract boolean mayContain(Object filter, Object potential);
  }

  public static byte[] getBytes(BytesWritable bw) {
    byte[] ret = new byte[bw.getSize()];
    System.arraycopy(bw.get(), 0, ret, 0, bw.getSize());
    return ret;    
  }
}