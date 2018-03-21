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
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.WikiDocAbsContentHandler;
import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.WikiStats;

public class XmlDocAbsExtractMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final String ARTICLE_PREFIX = "Wikipedia: ";

  private final WikiDocAbsContentHandler contentHandler = new WikiDocAbsContentHandler();
  private final Text titleText = new Text();
  private final Text absText = new Text();

  private final XMLReader parser;

  public XmlDocAbsExtractMapper() {
    try {
      parser = XMLReaderFactory.createXMLReader();
      parser.setContentHandler(contentHandler);
    } catch (SAXException e) {
      throw new IllegalStateException("Unable to create SAX parser", e);
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Reader reader = new StringReader(value.toString());
    try {
      parser.parse(new InputSource(reader));
      String title = contentHandler.getTitle();
      String absTextContent = contentHandler.getAbsText();
      if (absTextContent != null && StringUtils.startsWith(title, ARTICLE_PREFIX)) {
        title = StringUtils.remove(title, ARTICLE_PREFIX);
        titleText.set(title);
        absText.set(absTextContent);

        context.write(titleText, absText);
      } else {
        context.getCounter(WikiStats.NON_TARGET_NAMESPACE);
      }
    } catch (SAXException e) {
      context.getCounter(WikiStats.PARSING_EXCEPTION).increment(1L);
    } finally {
      contentHandler.resetMembers();
    }
  }

}
