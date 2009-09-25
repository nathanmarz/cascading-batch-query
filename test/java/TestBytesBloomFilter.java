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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestBytesBloomFilter extends TestCase {
  public void testSetSanity() throws IOException {
    FileSystem local = FileSystem.getLocal(new Configuration());
    
    BytesBloomFilter set = new BytesBloomFilter(1000000, 4);
    byte[] arr1 = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
    byte[] arr2 = new byte[] { 11, 12, 5, -2 };
    byte[] arr3 = new byte[] { 3, 4, 5 };
    set.add(arr1);
    set.add(arr2);
    
    for(byte i=0; i<(byte)125; i++) {
      set.add(new byte[] {i});
    }
    
    assertTrue(set.mayContain(arr1));
    assertTrue(set.mayContain(arr2));

    for(byte i=0; i<(byte)125; i++) {
      assertTrue(set.mayContain(new byte[] {i}));
    }

    //technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set.mayContain(arr3));
    
    //now test that we can write and read from file just fine
    local.delete(new Path("/tmp/filter-test.bloomfilter"), false);
    DataOutputStream os = new DataOutputStream(new FileOutputStream("/tmp/filter-test.bloomfilter"));
    set.write(os);
    os.close();
    
    BytesBloomFilter set2 = new BytesBloomFilter();
    DataInputStream is = new DataInputStream(new FileInputStream("/tmp/filter-test.bloomfilter"));
    set2.readFields(is);
    
    
    assertTrue(set2.mayContain(arr1));
    assertTrue(set2.mayContain(arr2));

    for(byte i=0; i<(byte)125; i++) {
      assertTrue(set2.mayContain(new byte[] {i}));
    }

    //technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set2.mayContain(arr3));
    
  }
}