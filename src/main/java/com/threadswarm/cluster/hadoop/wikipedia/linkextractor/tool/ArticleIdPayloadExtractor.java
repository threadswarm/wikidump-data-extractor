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
package com.threadswarm.cluster.hadoop.wikipedia.linkextractor.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.mapred.ArticleIdPayloadMapper;

public class ArticleIdPayloadExtractor extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    int result = ToolRunner.run(config, new ArticleIdPayloadExtractor(), args);
    System.exit(result);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration config = getConf();

    Integer payloadIndex = Integer.parseInt(args[2]);
    config.set(ArticleIdPayloadMapper.PAYLOAD_INDEX_KEY, payloadIndex.toString());

    Job job = Job.getInstance(config);

    job.setJobName("Wiki article-id/payload extractor (" + payloadIndex + ")");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(ArticleIdPayloadMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
