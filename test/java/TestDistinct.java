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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class TestDistinct extends TestCase {
  public void testDistinct() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("/tmp/test_distinct_file"), true);
    fs.delete(new Path("/tmp/test_distinct_file_results"), true);
    
    FSDataOutputStream out = fs.create(new Path("/tmp/test_distinct_file"));
    PrintWriter pw = new PrintWriter(out);
    pw.println("distinct1");
    pw.println("distinct2");
    pw.println("distinct2");
    pw.println("distinct3");
    pw.println("distinct2");
    pw.flush();
    out.close();
    
    Map<String, Tap> sources = new HashMap<String, Tap>();
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    
    Tap inTap = new Hfs(new TextLine(new Fields("line")), "/tmp/test_distinct_file");
    Pipe inPipe = new Pipe("inPipe");
    sources.put("inPipe", inTap);
    
    Distinct distinct = new Distinct(inPipe);
    
    Tap outTap = new Hfs(new TextLine(new Fields("line")), "/tmp/test_distinct_file_results");
    Pipe outPipe = new Pipe("outPipe", distinct);
    sinks.put("outPipe", outTap);
    
    Flow flow = new FlowConnector().connect(sources, sinks, inPipe, outPipe);
    flow.complete();
    
    FSDataInputStream in = fs.open(new Path("/tmp/test_distinct_file_results/part-00000"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    
    ArrayList<String> results = new ArrayList<String>();
    results.add("distinct1");
    results.add("distinct2");
    results.add("distinct3");    
    
    try {
      while(true) {
        String s = reader.readLine();
        if (s == null) {
          break;
        } 

        assertEquals(results.remove(0), s);
      }      
    } catch (Exception e) {
      fail("Got an exception while trying to verify the results: " + e.toString());
    }
    
    assertEquals("All results must be consumed!", 0, results.size());
  }
}