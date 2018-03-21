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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class XmlDocAbsExtractMapperTest {

  private final File sampleArticleFile = new File("src/test/resources/xml/sample_abstract.xml");

  private MapDriver<LongWritable, Text, Text, Text> mapDriver;

  @Before
  public void setup() {
    mapDriver = MapDriver.newMapDriver(new XmlDocAbsExtractMapper());
  }

  @Test
  public void testMapper() throws IOException {
    String inputString = FileUtils.readFileToString(sampleArticleFile, StandardCharsets.UTF_8);
    mapDriver.withInput(new LongWritable(), new Text(inputString));
    mapDriver.withOutput(new Text("Castello di Roccabianca"),
        new Text("The Castello or Castle of Roccabianca, is also called Rocca dei Rossi, "
            + "is a castle built in the town of Roccabianca, Province of Parma, region of Emilia Romagna, Italy."));

    mapDriver.runTest();
  }

}
