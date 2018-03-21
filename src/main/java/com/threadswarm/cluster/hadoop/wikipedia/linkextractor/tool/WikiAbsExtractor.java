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

import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.XmlInputFormat;
import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.mapred.FirstKeyValueReducer;
import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.mapred.XmlDocAbsExtractMapper;

public class WikiAbsExtractor extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    int result = ToolRunner.run(config, new WikiAbsExtractor(), args);
    System.exit(result);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration config = getConf();
    config.set("xmlinput.start", "<doc>");
    config.set("xmlinput.end", "</doc>");

    Job job = Job.getInstance(config);

    job.setJobName("Wiki abstract extractor");
    job.setJarByClass(getClass());
    job.setInputFormatClass(XmlInputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(XmlDocAbsExtractMapper.class);
    job.setCombinerClass(FirstKeyValueReducer.class);
    job.setReducerClass(FirstKeyValueReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
