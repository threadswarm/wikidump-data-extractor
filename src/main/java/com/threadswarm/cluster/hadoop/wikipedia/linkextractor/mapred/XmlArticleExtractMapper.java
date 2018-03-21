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
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.WikiPageContentHandler;
import com.threadswarm.cluster.hadoop.wikipedia.linkextractor.WikiStats;

public class XmlArticleExtractMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final static String DEFAULT_WIKI_NAMESPACE = "0";
  private final static String TAB = "\t";

  private final Pattern articleLinkPattern = Pattern.compile("\\[\\[([\\w\\s]+?)(\\|[\\w\\s]+?)??\\]\\]");

  private final WikiPageContentHandler contentHandler = new WikiPageContentHandler();
  private final Text titleAndLinkText = new Text();
  private final Text idText = new Text();

  private final XMLReader parser;

  public XmlArticleExtractMapper() {
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
      if (DEFAULT_WIKI_NAMESPACE.equals(contentHandler.getNamespace())) {
        String id = contentHandler.getId();
        if (id != null && !id.isEmpty()) {
          idText.set(contentHandler.getId());

          StringBuilder payloadBuilder = new StringBuilder(contentHandler.getTitle());
          String textContent = contentHandler.getTextContent();
          String articleLinkString = null;
          if (textContent != null && (articleLinkString = getArticleLinkString(textContent)) != null) {
            payloadBuilder.append(TAB);
            payloadBuilder.append(articleLinkString);
          }

          titleAndLinkText.set(payloadBuilder.toString());

          context.write(idText, titleAndLinkText);
          context.getCounter(WikiStats.TARGET_ARTICLE).increment(1);
        }
      } else {
        context.getCounter(WikiStats.NON_TARGET_NAMESPACE).increment(1L);
      }

    } catch (SAXException e) {
      context.getCounter(WikiStats.PARSING_EXCEPTION).increment(1L);
    } finally {
      contentHandler.resetMembers();
    }
  }

  private String getArticleLinkString(String textBody) {
    Set<String> articleLinkSet = getArticleLinkSet(textBody);
    StringBuilder articleLinkStringBuilder = new StringBuilder();
    int iterCount = 0;
    for (String articleLink : articleLinkSet) {
      if (iterCount++ > 0) {
        articleLinkStringBuilder.append("\t");
      }
      articleLinkStringBuilder.append(articleLink);
    }

    return articleLinkStringBuilder.toString();
  }

  private Set<String> getArticleLinkSet(String textBody) {
    Set<String> articleLinkSet = new TreeSet<>();

    Matcher matcher = articleLinkPattern.matcher(textBody);
    while (matcher.find()) {
      String articleLink = matcher.group(1);
      articleLinkSet.add(articleLink);
    }

    return articleLinkSet;
  }

}
