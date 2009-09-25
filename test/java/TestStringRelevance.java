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
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class TestStringRelevance extends TestCase {

  FileSystem fs;
  
  public TestStringRelevance() throws IOException {
    fs = FileSystem.get(new Configuration());
    Relevance.TEST_MODE = true;
  }
  
  public void testDummy() {
    
  }

  protected static class TestFunc extends Relevance.RelevanceFunction {
    @Override
    protected List<Object> extractPotentialMatches(TupleEntry tuple) {
      List<Object> ret = new ArrayList<Object>();
      ret.add(tuple.getString("str1"));
      ret.add(tuple.getString("str2"));
      return ret;
    }
  }
  
  protected static class GetRHS extends Relevance.RelevanceFunction {
    @Override
    protected List<Object> extractPotentialMatches(TupleEntry tuple) {
      List<Object> ret = new ArrayList<Object>();
      ret.add(tuple.getString("str2"));
      return ret;
    }
    
    @Override
    public boolean canHaveMultipleMatches() {
      return false;
    }
  }
  
  protected static class BadFunction extends Relevance.RelevanceFunction {
    @Override
    protected List<Object> extractPotentialMatches(TupleEntry tuple) {
      List<Object> ret = new ArrayList<Object>();
      ret.add(tuple.getString("str1"));
      ret.add(tuple.getString("str2"));
      return ret;
    }
    
    @Override
    public boolean canHaveMultipleMatches() {
      return false;
    }
  }

  final String INPUT = "/tmp/relevance_test_input";
  final String QUERY = "/tmp/relevance_query";
  final String OUTPUT = "/tmp/relevance_test_output";
  
  Tap inputTap;
  Tap outputTap;
  Tuple tuple1 = new Tuple("nathan@rapleaf.com", "alice@rapleaf.com");
  Tuple tuple2 = new Tuple("1@gmail.com", "2@gmail.com");
  Tuple tuple3 = new Tuple("2@gmail.com", "3@gmail.com");
  Tuple tuple4 = new Tuple("4@gmail.com", "4@gmail.com");
  Tuple tuple5 = new Tuple("5@gmail.com", "6@gmail.com");
  Tuple tuple6 = new Tuple("7@gmail.com", "8@gmail.com");
  Tuple tuple7 = new Tuple("77@gmail.com", "88@gmail.com");
  Tuple tuple8 = new Tuple("99@gmail.com", "88@gmail.com");
  Tuple tuple9 = new Tuple("1000@gmail.com", "8888@gmail.com");
  Tap keyTap;
  
  @Override
  public void setUp() throws Exception {
    fs.delete(new Path(INPUT), true);
    fs.delete(new Path(QUERY), true);
    fs.delete(new Path(OUTPUT), true);
    
    inputTap = new Hfs(new SequenceFile(new Fields("str1", "str2")), INPUT);
    TapCollector coll = new TapCollector(inputTap, new JobConf());
    coll.add(tuple1);
    coll.add(tuple2);
    coll.add(tuple3);
    coll.add(tuple4);
    coll.add(tuple5);
    coll.add(tuple6);
    coll.add(tuple7);
    coll.add(tuple8);
    coll.add(tuple9);
    coll.close();
    
    
    keyTap = new Hfs(new SequenceFile(new Fields("str")), QUERY);
    coll = new TapCollector(keyTap, new JobConf());
    coll.add(new Tuple(new Text("nathan@rapleaf.com")));
    coll.add(new Tuple(new Text("1@gmail.com")));
    coll.add(new Tuple(new Text("2@gmail.com")));
    coll.add(new Tuple(new Text("6@gmail.com")));
    coll.close();
    
    outputTap = new Hfs(new SequenceFile(new Fields("str1", "str2")), OUTPUT);    
  }
  
  public static List<Tuple> getAllTuples(Tap tap) throws IOException {
    TapIterator it = new TapIterator(tap, new JobConf());
    List<Tuple> ret = new ArrayList<Tuple>();
    while(it.hasNext()) {
      Tuple t = new Tuple(it.next()); //need to copy it since TapIterator reuses the same tuple object
      ret.add(t);
    }
    return ret;
  }

  public void testItNonexact() throws Exception {

    
    new StringRelevance().batch_query(inputTap, outputTap, new Fields("str1", "str2"), new TestFunc(), keyTap, "str", 24, 2, false);
    
    List<Tuple> tuples = getAllTuples(outputTap);
    System.out.println(tuples.size());
    assertTrue(tuples.size()>=4 && tuples.size() <= 9);
    assertTrue(tuples.contains(tuple1));
    assertTrue(tuples.contains(tuple2));
    assertTrue(tuples.contains(tuple3));
    assertTrue(tuples.contains(tuple5));
    
    
  }
  
  public void testItExact() throws Exception {
    new StringRelevance().batch_query(inputTap, outputTap, new Fields("str1", "str2"), new TestFunc(), keyTap, "str", 40, 2, true);
    List<Tuple> tuples = getAllTuples(outputTap);
    assertEquals(4, tuples.size());
    assertTrue(tuples.contains(tuple1));
    assertTrue(tuples.contains(tuple2));
    assertTrue(tuples.contains(tuple3));
    assertTrue(tuples.contains(tuple5));
    assertFalse(tuples.contains(tuple7));
  }
  
  public void testBadRelevanceFunction() throws Exception {
    try {
      new StringRelevance().batch_query(inputTap, outputTap, new Fields("str1", "str2"), new BadFunction(), keyTap, "str", 10, 2, true);
      fail("should throw exception");
    } catch(Exception e) {
      
    }
  }
  
    
}