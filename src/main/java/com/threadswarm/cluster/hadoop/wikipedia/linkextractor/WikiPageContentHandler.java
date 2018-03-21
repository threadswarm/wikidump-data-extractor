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

public class WikiPageContentHandler extends DefaultHandler {

  private final Map<String, CurrentPageElement> pageElementMap = new HashMap<>();

  private String namespace = null;
  private String id = null;
  private String title = null;
  private String textContent = null;

  private CurrentPageElement currentPageElement = null;
  private StringBuilder valueBuilder = null;
  private boolean inRevision = false;

  public WikiPageContentHandler() {
    for (CurrentPageElement element : CurrentPageElement.values()) {
      pageElementMap.put(element.getTagName(), element);
    }
  }

  public String getNamespace() {
    return namespace;
  }

  public String getId() {
    return id;
  }

  public String getTitle() {
    return title;
  }

  public String getTextContent() {
    return textContent;
  }

  public void resetMembers() {
    namespace = null;
    id = null;
    title = null;
    textContent = null;

    valueBuilder = null;
    currentPageElement = null;
    inRevision = false;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    currentPageElement = pageElementMap.get(localName);
    if (CurrentPageElement.REVISION == currentPageElement) {
      inRevision = true;
    }
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

  @SuppressWarnings("incomplete-switch")
  @Override
  public void endElement(String uri, String localName, String qName) {
    if (currentPageElement != null) {
      if (CurrentPageElement.REVISION == currentPageElement) {
        inRevision = false;
      } else if (valueBuilder != null) {
        if (inRevision) {
          if (CurrentPageElement.TEXT == currentPageElement)
            textContent = valueBuilder.toString();
        } else {
          switch (currentPageElement) {
            case NAMESPACE:
              namespace = valueBuilder.toString().trim();
              break;
            case ID:
              id = valueBuilder.toString().trim();
              break;
            case TITLE:
              title = valueBuilder.toString().trim();
              break;
          }
        }
        valueBuilder = null;
      }

      currentPageElement = null;
    }
  }

  private static enum CurrentPageElement {

    NAMESPACE("ns"), ID("id"), TITLE("title"), TEXT("text"), REVISION("revision");

    private final String tagName;

    private CurrentPageElement(String tagName) {
      this.tagName = tagName;
    }

    public String getTagName() {
      return tagName;
    }
  }

}
