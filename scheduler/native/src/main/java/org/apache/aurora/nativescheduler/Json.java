/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.nativescheduler;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

final class Json {
  static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  static final int LIMIT = 1024 * 1024;
  static JsonNode parse(byte[] data) throws IOException {
    if (data.length > LIMIT) { throw new IOException("JSON too large"); }
    JsonNode value = MAPPER.readTree(data);
    if (value == null) { throw new IOException("Missing JSON"); }
    return value;
  }
  static JsonNode parse(String data) throws IOException {
    return parse(data.getBytes(StandardCharsets.UTF_8));
  }
  static byte[] read(InputStream input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    for (int count; (count = input.read(buffer)) != -1;) {
      if (out.size() + count > LIMIT) {
        throw new IOException("Body too large");
      }
      out.write(buffer, 0, count);
    }
    return out.toByteArray();
  }
  static ObjectNode object() { return MAPPER.createObjectNode(); }
  static ArrayNode array() { return MAPPER.createArrayNode(); }
  static JsonNode sorted(JsonNode value) {
    if (value.isObject()) {
      ObjectNode result = object(); List<String> keys = new ArrayList<>();
      value.fieldNames().forEachRemaining(keys::add); Collections.sort(keys);
      for (String key : keys) { result.set(key, sorted(value.get(key))); }
      return result;
    }
    if (value.isArray()) {
      ArrayNode result = array(); for (JsonNode element : value) { result.add(sorted(element)); }
      return result;
    }
    return value;
  }
  static String canonical(JsonNode value) { return sorted(value).toString(); }
  static String sha(String value) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    StringBuilder result = new StringBuilder();
    for (byte b : digest) {
      result.append(String.format(Locale.ROOT, "%02x", b & 255));
    }
    return result.toString();
  }
  static String string(JsonNode node, String field) {
    if (!node.path(field).isTextual()) { throw new IllegalArgumentException("Missing string: " + field); }
    return node.get(field).textValue();
  }
  static void fields(JsonNode value, String... fields) {
    if (!value.isObject()) { throw new IllegalArgumentException("Expected object"); }
    Set<String> expected = new HashSet<>(Arrays.asList(fields));
    value.fieldNames().forEachRemaining(key -> {
      if (!expected.remove(key)) { throw new IllegalArgumentException("Unknown field"); }
    });
    if (!expected.isEmpty()) { throw new IllegalArgumentException("Missing fields"); }
  }
}
