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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import cascading.tap.Tap;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class StringRelevance extends Relevance {

  
  protected static class StringRelevanceFilter extends RelevanceFilter {

    public StringRelevanceFilter(RelevanceFunction func, String job_id) {
      super(func, job_id);
    }
    
    @Override
    protected Object loadFilter(FileSystem fs, String filterFile) throws IOException {
      return BytesBloomFilter.readFromFileSystem(fs, new Path(filterFile));
    }

    @Override
    protected boolean mayContain(Object filter, Object potential) {
      BytesBloomFilter bbf = (BytesBloomFilter) filter;
      return bbf.mayContain(((String) potential).getBytes());
    }
    
  }
  
  protected static class StringPrepareExactFilter extends PrepareExactFilter {
    public StringPrepareExactFilter(RelevanceFunction func) {
      super(func);
    }
    
    @Override
    protected WritableComparable toWritableComparable(Object relevant) {
      return new Text((String)relevant);
    }
    
  }
  
  protected static class StringExtractionFunction extends ExtractionFunction {
    public StringExtractionFunction(RelevanceFunction func, String out_field) {
      super(func, out_field);
    }
    
    @Override
    protected WritableComparable toWritableComparable(Object relevant) {
      return new Text((String) relevant);
    }
    
  }
  
  
  @Override
  protected PrepareExactFilter getExactFilter(RelevanceFunction func) {
    return new StringPrepareExactFilter(func);
  }

  @Override
  protected RelevanceFilter getRelevanceFilter(RelevanceFunction func, String job_id) {
    return new StringRelevanceFilter(func, job_id);
  }
  
  @Override
  protected ExtractionFunction getExtractionFunction(RelevanceFunction func, String out_field) {
    return new StringExtractionFunction(func, out_field);
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
      String s = t.getString(keyField);
      filter.add(s.getBytes());
    }
    it.close();
    filter.writeToFileSystem(fs, new Path(path));

  }


  
}