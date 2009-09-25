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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import cascading.tap.Tap;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class BytesRelevance extends Relevance {

  protected static class BytesRelevanceFilter extends RelevanceFilter {

    public BytesRelevanceFilter(RelevanceFunction func, String job_id) {
      super(func, job_id);
    }

    @Override
    protected Object loadFilter(FileSystem fs, String filterFile)
        throws IOException {
      return BytesBloomFilter.readFromFileSystem(fs, new Path(filterFile));
    }

    @Override
    protected boolean mayContain(Object filter, Object potential) {
      BytesBloomFilter bbf = (BytesBloomFilter) filter;
      return bbf.mayContain((byte[]) potential);
    }

  }

  protected static class BytesPrepareExactFilter extends PrepareExactFilter {
    public BytesPrepareExactFilter(RelevanceFunction func) {
      super(func);
    }

    @Override
    protected WritableComparable toWritableComparable(Object relevant) {
      return new BytesWritable((byte[]) relevant);
    }

  }

  protected static class BytesExtractionFunction extends ExtractionFunction {
    public BytesExtractionFunction(RelevanceFunction func, String out_field) {
      super(func, out_field);
    }

    @Override
    protected WritableComparable toWritableComparable(Object relevant) {
      return new BytesWritable((byte[]) relevant);
    }

  }

  @Override
  protected PrepareExactFilter getExactFilter(RelevanceFunction func) {
    return new BytesPrepareExactFilter(func);
  }

  @Override
  protected RelevanceFilter getRelevanceFilter(RelevanceFunction func,
      String job_id) {
    return new BytesRelevanceFilter(func, job_id);
  }

  @Override
  protected ExtractionFunction getExtractionFunction(RelevanceFunction func,
      String out_field) {
    return new BytesExtractionFunction(func, out_field);
  }

  @Override
  protected void writeOutBloomFilter(Tap keysTap, String keyField,
      FileSystem fs, String path, int bloom_bits, int bloom_hashes)
      throws IOException {
    BytesBloomFilter filter = new BytesBloomFilter(bloom_bits, bloom_hashes);
    TupleEntryIterator it = new TupleEntryIterator(keysTap.getSourceFields(),
        new TapIterator(keysTap, new JobConf()));
    while (it.hasNext()) {
      TupleEntry t = it.next();
      byte[] b = getBytes((BytesWritable) t.get("keyField"));
      filter.add(b);
    }
    it.close();
    filter.writeToFileSystem(fs, new Path(path));

  }

}