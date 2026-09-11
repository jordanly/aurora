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
package org.apache.aurora.nativeprotocol;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;

/** Strict wire validation. Authentication, state transitions and admission are separate. */
public final class ProtocolValidator {
  public static final int MAX_BYTES = 1024 * 1024;
  private static final BigInteger MAX_INTEGER = new BigInteger("9007199254740991");
  private static final BigInteger MAX_COUNTER = new BigInteger("18446744073709551615");
  private static final Set<String> COUNTERS = new HashSet<>(Arrays.asList(
      "revision", "desiredRevision", "schedulerEpoch", "sequence", "cursor",
      "generation", "watermark", "committedCursor"));
  private final ObjectMapper mapper;
  private final Schema schema;

  public ProtocolValidator() throws IOException {
    JsonFactory factory = new JsonFactory();
    factory.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    mapper = new ObjectMapper(factory).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    try (InputStream stream = ProtocolValidator.class.getResourceAsStream("/schema.json")) {
      require(stream != null, "missing bundled native schema");
      JsonNode definition = mapper.readTree(stream);
      localReferencesOnly(definition);
      schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
          .getSchema(definition);
    }
  }

  /** Returns an immutable validation result; callers receive copies of its mutable data. */
  public Message validate(byte[] input) throws IOException {
    require(input.length <= MAX_BYTES, "document exceeds 1 MiB");
    // Do not let Jackson auto-detect UTF-16/32 or strip a BOM that other peers reject.
    // This bounded profile permits ASCII JSON bytes; escaped ASCII strings remain valid.
    for (byte value : input) {
      int ch = value & 0xff;
      require((ch >= 32 && ch <= 126) || ch == 9 || ch == 10 || ch == 13,
          "expected ASCII JSON in UTF-8 without BOM");
    }
    // Check original number tokens before tree parsing can normalize -0 or 1e0.
    try (JsonParser parser = mapper.getFactory().createParser(input)) {
      int depth = 0;
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
          require(++depth <= 64, "document exceeds nesting limit");
        } else if (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
          depth--;
        } else if (token == JsonToken.FIELD_NAME || token == JsonToken.VALUE_STRING) {
          ascii(parser.getText());
        } else if (token == JsonToken.VALUE_NUMBER_INT) {
          String number = parser.getText();
          require(number.matches("0|[1-9][0-9]*")
              && new BigInteger(number).compareTo(MAX_INTEGER) <= 0, "invalid JSON integer");
        } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
          throw new IllegalArgumentException("fractional/exponent numbers are unsupported");
        }
      }
    }
    JsonNode value = mapper.readTree(input);
    require(value != null && schema.validate(value).isEmpty(), "native schema validation failed");
    semantics(value);
    return new Message(value, canonical(value));
  }

  private static void ascii(String value) {
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      require(ch >= 32 && ch <= 126, "only printable ASCII is supported");
    }
  }

  private static void localReferencesOnly(JsonNode value) {
    if (value.isObject()) {
      for (Map.Entry<String, JsonNode> field : value.properties()) {
        if (field.getKey().equals("$ref")) {
          require(field.getValue().asText().startsWith("#/"), "external schema reference");
        }
        localReferencesOnly(field.getValue());
      }
    } else if (value.isArray()) {
      value.forEach(ProtocolValidator::localReferencesOnly);
    }
  }

  private static void counters(JsonNode value) {
    if (value.isObject()) {
      for (Map.Entry<String, JsonNode> field : value.properties()) {
        if (COUNTERS.contains(field.getKey())) {
          require(new BigInteger(field.getValue().asText()).compareTo(MAX_COUNTER) <= 0,
              "counter overflow");
        }
        counters(field.getValue());
      }
    } else if (value.isArray()) {
      value.forEach(ProtocolValidator::counters);
    }
  }

  private static void semantics(JsonNode value) {
    counters(value);
    String kind = value.get("kind").asText();
    JsonNode process = value.has("template") ? value.get("template") : value.get("assignment");
    if (process != null) {
      require(process.get("argv").get(0).isTextual()
          && process.get("argv").get(0).asText().startsWith("/"), "executable must be absolute");
      Set<String> capabilities = new HashSet<>();
      process.get("requiredCapabilities").forEach(c -> capabilities.add(c.asText()));
      require(!process.get("resources").get("memoryEnforcement").asText().equals("hard")
          || capabilities.contains("hard-memory"), "hard memory needs capability requirement");
      Set<String> names = new HashSet<>();
      Set<List<String>> sockets = new HashSet<>();
      for (JsonNode port : process.get("ports")) {
        require(names.add(port.get("name").asText()), "duplicate port name");
        if (kind.equals("Run")) {
          require(sockets.add(Arrays.asList(port.get("network").asText(),
              port.get("protocol").asText(), port.get("family").asText(),
              port.get("number").asText())), "duplicate assigned socket");
        }
      }
      for (JsonNode argument : process.get("argv")) {
        require(!argument.isObject() || names.contains(argument.get("portRef").asText()),
            "unknown argv port reference");
      }
      JsonNode readiness = process.get("readiness");
      require(!readiness.get("kind").asText().equals("tcp")
          || names.contains(readiness.get("port").asText()), "unknown readiness port");
      require(!kind.equals("Run")
          || value.get("identity").get("process").equals(process.get("process")),
          "assignment process differs from identity");
    }
    if (kind.equals("Delivery")) {
      JsonNode body = value.get("body");
      for (String key : Arrays.asList("cluster", "incarnation")) {
        require(value.get("authority").get(key).equals(body.get("identity").get(key)),
            "authority scope differs from body");
      }
      require(value.get("bodySha256").asText().equals(hash(canonical(body))), "body hash mismatch");
      semantics(body);
    }
  }

  /** Checks actual agent capability availability separately from manifest structure. */
  public static void requireCapabilities(Message message, Set<String> advertised) {
    JsonNode value = message.value;
    if (value.get("kind").asText().equals("Delivery")) {
      value = value.get("body");
    }
    JsonNode process = value.has("template") ? value.get("template") : value.get("assignment");
    require(process != null, "message has no process requirements");
    for (JsonNode capability : process.get("requiredCapabilities")) {
      require(advertised.contains(capability.asText()), "missing agent capability");
    }
  }

  /** Verifies that a validated assignment is a resolution of the named desired Job revision. */
  public static void requireResolution(Message jobMessage, Message runMessage) {
    JsonNode job = jobMessage.value;
    JsonNode run = runMessage.value;
    require(job.get("kind").asText().equals("Job") && run.get("kind").asText().equals("Run"),
        "resolution requires Job and Run");
    for (String key : Arrays.asList("cluster", "incarnation", "jobKey")) {
      require(job.get(key).equals(run.get("identity").get(key)), "job identity mismatch");
    }
    require(job.get("revision").equals(run.get("desiredRevision")), "job revision mismatch");
    require(hash(canonical(job.get("template"))).equals(run.get("templateSha256").asText()),
        "template digest mismatch");
    ObjectNode resolved = job.get("template").deepCopy();
    JsonNode assignments = run.get("assignment").get("ports");
    require(assignments.size() == resolved.get("ports").size(), "port declaration count mismatch");
    for (int index = 0; index < assignments.size(); index++) {
      ObjectNode declaration = assignments.get(index).deepCopy();
      declaration.remove(Arrays.asList("number", "network"));
      require(declaration.equals(resolved.get("ports").get(index)), "port declaration mismatch");
    }
    ArrayNode arguments = resolved.putArray("argv");
    for (JsonNode argument : job.get("template").get("argv")) {
      if (argument.isTextual()) {
        arguments.add(argument.asText());
      } else {
        for (JsonNode port : assignments) {
          if (port.get("name").equals(argument.get("portRef"))) {
            arguments.add(port.get("number").asText());
          }
        }
      }
    }
    resolved.set("ports", assignments.deepCopy());
    require(resolved.equals(run.get("assignment")), "assignment does not resolve template");
  }

  private static byte[] canonical(JsonNode value) {
    StringBuilder result = new StringBuilder();
    encode(value, result);
    return result.toString().getBytes(StandardCharsets.US_ASCII);
  }

  private static void encode(JsonNode value, StringBuilder output) {
    if (value.isObject()) {
      List<String> keys = new ArrayList<>();
      value.fieldNames().forEachRemaining(keys::add);
      Collections.sort(keys);
      output.append('{');
      boolean first = true;
      for (String key : keys) {
        if (!first) { output.append(','); }
        first = false;
        quote(key, output);
        output.append(':');
        encode(value.get(key), output);
      }
      output.append('}');
    } else if (value.isArray()) {
      output.append('[');
      boolean first = true;
      for (JsonNode item : value) {
        if (!first) { output.append(','); }
        first = false;
        encode(item, output);
      }
      output.append(']');
    } else if (value.isTextual()) {
      quote(value.asText(), output);
    } else {
      output.append(value.toString());
    }
  }

  private static void quote(String value, StringBuilder output) {
    output.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
  }

  private static String hash(byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder result = new StringBuilder();
      for (byte value : digest) {
        result.append(String.format("%02x", value & 0xff));
      }
      return result.toString();
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) { throw new IllegalArgumentException(message); }
  }

  public static final class Message {
    private final JsonNode value;
    private final byte[] bytes;
    private Message(JsonNode value, byte[] bytes) {
      this.value = value;
      this.bytes = bytes;
    }
    public JsonNode json() { return value.deepCopy(); }
    public byte[] canonicalBytes() { return bytes.clone(); }
    public String sha256() { return hash(bytes); }
  }
}
