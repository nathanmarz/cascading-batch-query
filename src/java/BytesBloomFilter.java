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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;


public class BytesBloomFilter implements Writable {
  private BloomFilter _filter;

  
  public static BytesBloomFilter readFromFileSystem(FileSystem fs, Path p) throws IOException {
    BytesBloomFilter ret = new BytesBloomFilter();
    FSDataInputStream is = fs.open(p);
    ret.readFields(is);
    is.close();
    return ret;
  }
  
  public BytesBloomFilter() {
    _filter = new BloomFilter();
  }
  
  public BytesBloomFilter(int vectorLength, int numHashes) {
    _filter = new BloomFilter(vectorLength, numHashes, Hash.MURMUR_HASH);
  }

  public void add(byte[] bytes) {
    _filter.add(new Key(bytes));
  }

  public boolean mayContain(byte[] bytes) {
    return _filter.membershipTest(new Key(bytes));
  }
  
  public double falsePositiveRate() {
    return _filter.getFalsePositiveRate();
  }
  
  public void acceptAll() {
    _filter.acceptAll();
  }
  
  public void readFields(DataInput in) throws IOException {
    _filter.readFields(in);    
  }

  public void write(DataOutput out) throws IOException {
    _filter.write(out);
  }
  
  public void writeToFileSystem(FileSystem fs, Path p) throws IOException {
    FSDataOutputStream os = fs.create(p);
    write(os);
    os.close();
  }
}