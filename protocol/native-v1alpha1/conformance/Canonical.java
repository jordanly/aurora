/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* Standalone Java 8 canonical adapter, deliberately independent of legacy Gradle.
 * This is a bounded fixture parser, not a production protocol implementation.
 * Schema and semantic validation remain in check.py. */
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class Canonical {
  private final String text;
  private int offset;
  private Canonical(String text) { this.text = text; }
  private IllegalArgumentException error() {
    return new IllegalArgumentException("Invalid bounded JSON at " + offset);
  }
  private void space() {
    while (offset < text.length() && " \t\n\r".indexOf(text.charAt(offset)) >= 0) offset++;
  }
  private char take() { if (offset == text.length()) throw error(); return text.charAt(offset++); }
  private boolean eat(char c) {
    space(); if (offset < text.length() && text.charAt(offset) == c) { offset++; return true; }
    return false;
  }
  private String string() {
    if (take() != '"') throw error();
    StringBuilder result = new StringBuilder();
    while (true) {
      char c = take();
      if (c == '"') return result.toString();
      if (c == '\\') {
        c = take();
        if (c == 'u') {
          if (offset + 4 > text.length()) throw error();
          for (int index = offset; index < offset + 4; index++) {
            if ("0123456789abcdefABCDEF".indexOf(text.charAt(index)) < 0) throw error();
          }
          c = (char) Integer.parseInt(text.substring(offset, offset + 4), 16); offset += 4;
        } else if (c != '"' && c != '\\' && c != '/') throw error();
      }
      if (c < 32 || c > 126) throw error();
      result.append(c);
    }
  }
  private Object value() {
    space(); if (offset == text.length()) throw error();
    char c = text.charAt(offset);
    if (c == '"') return string();
    if (eat('{')) {
      Map<String, Object> map = new TreeMap<>();
      if (eat('}')) return map;
      do {
        space(); String key = string(); if (!eat(':') || map.containsKey(key)) throw error();
        map.put(key, value());
      } while (eat(','));
      if (!eat('}')) throw error(); return map;
    }
    if (eat('[')) {
      List<Object> list = new ArrayList<>(); if (eat(']')) return list;
      do { list.add(value()); } while (eat(','));
      if (!eat(']')) throw error(); return list;
    }
    for (String literal : new String[]{"true", "false", "null"}) {
      if (text.startsWith(literal, offset)) {
        offset += literal.length();
        return literal.equals("null") ? null : Boolean.valueOf(literal);
      }
    }
    int start = offset;
    while (offset < text.length() && text.charAt(offset) >= '0' && text.charAt(offset) <= '9') offset++;
    if (start == offset || (offset - start > 1 && text.charAt(start) == '0')) throw error();
    long number = Long.parseLong(text.substring(start, offset));
    if (number > 9007199254740991L) throw error(); return number;
  }
  private static String quote(String text) {
    return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
  private static String encode(Object value) {
    if (value == null) return "null";
    if (value instanceof String) return quote((String) value);
    StringBuilder result = new StringBuilder();
    if (value instanceof Map) {
      result.append('{'); boolean first = true;
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        if (!first) result.append(','); first = false;
        result.append(quote((String) entry.getKey())).append(':').append(encode(entry.getValue()));
      }
      return result.append('}').toString();
    }
    if (value instanceof List) {
      result.append('['); boolean first = true;
      for (Object item : (List<?>) value) {
        if (!first) result.append(','); first = false; result.append(encode(item));
      }
      return result.append(']').toString();
    }
    return value.toString();
  }
  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("usage: Canonical document.json");
    byte[] bytes = Files.readAllBytes(Paths.get(args[0]));
    Canonical parser = new Canonical(new String(bytes, StandardCharsets.UTF_8));
    Object value = parser.value(); parser.space();
    if (parser.offset != parser.text.length()) throw parser.error();
    System.out.write((encode(value) + "\n").getBytes(StandardCharsets.US_ASCII));
  }
}
