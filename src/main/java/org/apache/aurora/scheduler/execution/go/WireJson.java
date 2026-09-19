/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.execution.go;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Bounded strict JSON and the agent protocol's sorted-key ASCII canonical encoding. */
final class WireJson {
  static final int MAX_BYTES = 1024 * 1024;
  private static final ObjectMapper MAPPER = new ObjectMapper(
      new JsonFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION));

  private WireJson() { }

  static ObjectNode object() {
    return MAPPER.createObjectNode();
  }

  static JsonNode parse(byte[] bytes) throws IOException {
    require(bytes.length <= MAX_BYTES, "JSON exceeds 1 MiB");
    try (JsonParser parser = MAPPER.getFactory().createParser(bytes)) {
      JsonNode value = MAPPER.readTree(parser);
      require(value != null && parser.nextToken() == null, "Invalid or trailing JSON");
      return value;
    }
  }

  static byte[] bytes(JsonNode value) {
    try {
      return MAPPER.writeValueAsBytes(sorted(value, 0));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  private enum WatchPath { ROOT, STATE, ATTEMPTS, ATTEMPT, EXECUTION, EXIT_CODE, OTHER }

  static void validate(JsonNode value) {
    validate(value, 0, WatchPath.OTHER);
  }

  static void validateWatchFrame(JsonNode frame) {
    validate(frame, 0, WatchPath.ROOT);
  }

  private static void validate(JsonNode value, int depth, WatchPath path) {
    require(depth <= 64, "JSON nesting limit");
    if (value.isObject()) {
      value.properties().forEach(entry -> {
        ascii(entry.getKey());
        WatchPath child = switch (path) {
          case ROOT -> "state".equals(entry.getKey()) ? WatchPath.STATE : WatchPath.OTHER;
          case STATE -> "attempts".equals(entry.getKey()) ? WatchPath.ATTEMPTS : WatchPath.OTHER;
          case ATTEMPTS -> WatchPath.ATTEMPT;
          case ATTEMPT -> "execution".equals(entry.getKey())
              ? WatchPath.EXECUTION : WatchPath.OTHER;
          case EXECUTION -> "exitCode".equals(entry.getKey())
              ? WatchPath.EXIT_CODE : WatchPath.OTHER;
          default -> WatchPath.OTHER;
        };
        validate(entry.getValue(), depth + 1, child);
      });
    } else if (value.isArray()) {
      value.forEach(child -> validate(child, depth + 1, WatchPath.OTHER));
    } else {
      validateScalar(value, path == WatchPath.EXIT_CODE);
    }
  }

  private static JsonNode sorted(JsonNode value, int depth) {
    require(depth <= 64, "JSON nesting limit");
    if (value.isObject()) {
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      value.properties().forEach(entry -> {
        ascii(entry.getKey());
        fields.put(entry.getKey(), sorted(entry.getValue(), depth + 1));
      });
      ObjectNode result = object();
      fields.forEach(result::set);
      return result;
    }
    if (value.isArray()) {
      var array = MAPPER.createArrayNode();
      value.forEach(item -> array.add(sorted(item, depth + 1)));
      return array;
    }
    validateScalar(value, false);
    return value;
  }

  private static void validateScalar(JsonNode value, boolean diagnosticExitCode) {
    if (value.isTextual()) {
      ascii(value.asText());
    } else if (value.isNumber()) {
      require(value.isIntegralNumber() && value.canConvertToLong()
          && (value.asLong() >= 0 || diagnosticExitCode && value.asLong() == -1)
          && value.asLong() <= 9007199254740991L,
          "Protocol JSON numbers must be safe nonnegative integers");
    }
  }

  static String hash(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  static String text(JsonNode value, String key) {
    JsonNode field = value.path(key);
    require(field.isTextual() && !field.asText().isEmpty(), "Missing string: " + key);
    ascii(field.asText());
    return field.asText();
  }

  static long counter(JsonNode value, String key) {
    String text = text(value, key);
    require(text.matches("0|[1-9][0-9]*"), "Invalid counter: " + key);
    return Long.parseLong(text);
  }

  static void fields(JsonNode value, String... names) {
    require(value.isObject(), "Expected object");
    Set<String> allowed = Set.of(names);
    value.fieldNames().forEachRemaining(
        key -> require(allowed.contains(key), "Unknown protocol JSON field"));
  }

  static String string(JsonNode value) {
    return new String(bytes(value), StandardCharsets.US_ASCII);
  }

  static ObjectNode base(String kind) {
    return object().put("version", "native-v1alpha1").put("kind", kind);
  }

  static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static void ascii(String text) {
    require(text.chars().allMatch(ch -> ch >= 32 && ch <= 126), "Printable ASCII required");
  }
}
