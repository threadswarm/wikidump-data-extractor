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
package com.threadswarm.cluster.hadoop.wikipedia.linkextractor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

public class WikiDocAbsContentHandler extends DefaultHandler {

  private final Map<String, CurrentPageElement> pageElementMap = new HashMap<>();

  private String title;
  private String absText = null;

  private CurrentPageElement currentPageElement = null;
  private StringBuilder valueBuilder = null;

  public WikiDocAbsContentHandler() {
    for (CurrentPageElement element : CurrentPageElement.values()) {
      pageElementMap.put(element.getTagName(), element);
    }
  }

  public String getTitle() {
    return title;
  }

  public String getAbsText() {
    return absText;
  }

  public void resetMembers() {
    title = null;
    absText = null;

    valueBuilder = null;
    currentPageElement = null;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    currentPageElement = pageElementMap.get(localName);

  }

  @Override
  public void characters(char[] ch, int start, int length) {
    if (length > 0 && currentPageElement != null) {
      char[] targetArray = Arrays.copyOfRange(ch, start, start + length);
      String value = String.valueOf(targetArray);

      if (valueBuilder == null) {
        valueBuilder = new StringBuilder(value);
      } else {
        valueBuilder.append(value);
      }
    }

  }

  @Override
  public void endElement(String uri, String localName, String qName) {
    if (currentPageElement != null) {
      if (valueBuilder != null) {
        switch (currentPageElement) {
          case TITLE:
            title = valueBuilder.toString().trim();
            break;

          case ABSTRACT:
            absText = valueBuilder.toString().trim();
            break;
        }
      }
      valueBuilder = null;
      currentPageElement = null;
    }
  }

  private static enum CurrentPageElement {

    TITLE("title"), ABSTRACT("abstract");

    private final String tagName;

    private CurrentPageElement(String tagName) {
      this.tagName = tagName;
    }

    public String getTagName() {
      return tagName;
    }

  }

}
