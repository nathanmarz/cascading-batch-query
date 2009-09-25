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

import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class Distinct extends SubAssembly {
  Pipe outPipe;
  
  public Distinct(Pipe pipe) {
    this(pipe, Fields.ALL);
  }
  
  public Distinct(Pipe pipe, Fields distinctFields) {
    // group on all values
    Pipe groupPipe = new GroupBy(pipe, distinctFields);
    // only take the first value in the grouping, ignore the rest
    outPipe = new Every(groupPipe, Fields.ALL, new FastFirst(), Fields.RESULTS);
    
    setTails(outPipe);
  }
}