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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class ArticleIdPayloadMapperTest {

  @Test
  public void testMapPayloadIndexTitle() throws IOException {
    MapDriver<LongWritable, Text, Text, Text> mapDriver = MapDriver.newMapDriver(new ArticleIdPayloadMapper());
    mapDriver.getConfiguration().set(ArticleIdPayloadMapper.PAYLOAD_INDEX_KEY, Integer.toString(1));
    mapDriver.withInput(new LongWritable(),
        new Text("48439033\tFootball (gridiron football)\t[[Shakespeare|Shakespearean]],[[XFL]]"));
    mapDriver.withOutput(new Text("48439033"), new Text("Football (gridiron football)"));

    mapDriver.runTest();
  }

  @Test
  public void testMapPayloadIndexLinks() throws IOException {
    MapDriver<LongWritable, Text, Text, Text> mapDriver = MapDriver.newMapDriver(new ArticleIdPayloadMapper());
    mapDriver.getConfiguration().set(ArticleIdPayloadMapper.PAYLOAD_INDEX_KEY, Integer.toString(2));
    mapDriver.withInput(new LongWritable(),
        new Text("48439033\tFootball (gridiron football)\t[[Shakespeare|Shakespearean]],[[XFL]]"));
    mapDriver.withOutput(new Text("48439033"), new Text("[[Shakespeare|Shakespearean]],[[XFL]]"));

    mapDriver.runTest();
  }

}
