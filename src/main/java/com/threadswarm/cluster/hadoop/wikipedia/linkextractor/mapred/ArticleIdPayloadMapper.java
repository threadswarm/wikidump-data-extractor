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
import org.apache.hadoop.mapreduce.Mapper;

public class ArticleIdPayloadMapper extends Mapper<LongWritable, Text, Text, Text> {

  public static final String PAYLOAD_INDEX_KEY = "PAYLOAD_INDEX";

  private final Text articleIdText = new Text();
  private final Text payloadText = new Text();

  private int payloadIndex;

  @Override
  protected void setup(Context context) {
    String payloadIndexString = context.getConfiguration().get(PAYLOAD_INDEX_KEY);
    payloadIndex = Integer.parseInt(payloadIndexString);
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokenArray = value.toString().split("\t");
    if (tokenArray.length > payloadIndex) {
      articleIdText.set(tokenArray[0]);
      payloadText.set(tokenArray[payloadIndex]);

      context.write(articleIdText, payloadText);
    }
  }

}
