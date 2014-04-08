package example;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestMapReduce {

  private MapDriver<LongWritable, DBInputWritable, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;

  @Before
  public void setUp() {
    Map mapper = new Map();
    Reduce reducer = new Reduce();
    mapDriver = MapDriver.newMapDriver(mapper);;
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }

 @Test
  public void testMapper() {
    DBInputWritable dbInputWritable = new DBInputWritable();
    dbInputWritable.setCustomer("ABC");
    dbInputWritable.setAmount(100);
    
	mapDriver.withInput(new LongWritable(1), dbInputWritable);
    mapDriver.withOutput(new Text("ABC"), new IntWritable(100));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(100));
    values.add(new IntWritable(200));
    values.add(new IntWritable(500));
    
    reduceDriver.withInput(new Text("ABC"), values);
    reduceDriver.withOutput(new Text("ABC"), 
        new Text("800"));
    reduceDriver.runTest();
  }
}
