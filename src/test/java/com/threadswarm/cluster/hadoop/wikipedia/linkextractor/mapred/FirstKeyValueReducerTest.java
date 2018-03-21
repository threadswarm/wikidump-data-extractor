/*
 * Copyright 2016 steve(at)threadswarm.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.threadswarm.cluster.hadoop.wikipedia.linkextractor.mapred;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class FirstKeyValueReducerTest {

  private ReduceDriver<Text, Text, Text, Text> reduceDriver;

  @Before
  public void setUp() throws Exception {
    reduceDriver = ReduceDriver.newReduceDriver(new FirstKeyValueReducer());
  }

  @Test
  public void testReduceWithMultipleValues() throws IOException {
    Text idKey = new Text("1234567");
    Text values = new Text("Football (gridiron football)\t[[Shakespeare|Shakespearean]],[[XFL]]");
    reduceDriver.withInput(idKey, Arrays.asList(values, values)).withOutput(idKey, values).runTest();
  }

  @Test
  public void testReduceWithSingleValue() throws IOException {
    Text idKey = new Text("1234567");
    Text values = new Text("Football (gridiron football)\t[[Shakespeare|Shakespearean]],[[XFL]]");
    reduceDriver.withInput(idKey, Collections.singletonList(values)).withOutput(idKey, values).runTest();
  }

}
