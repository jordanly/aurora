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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProtocolValidatorTest {
  private final Path fixtures = Paths.get(System.getProperty("aurora.fixtures"));
  private final ObjectMapper mapper = new ObjectMapper();
  private final ProtocolValidator validator = new ProtocolValidator();

  public ProtocolValidatorTest() throws IOException { }

  private ProtocolValidator.Message valid(String name) throws IOException {
    return validator.validate(Files.readAllBytes(fixtures.resolve("valid/" + name + ".json")));
  }

  private List<Path> corpus(String name) throws IOException {
    try (Stream<Path> paths = Files.list(fixtures.resolve(name))) {
      return paths.filter(p -> p.toString().endsWith(".json")).sorted().collect(Collectors.toList());
    }
  }

  private void rejects(byte[] bytes) throws IOException {
    try {
      validator.validate(bytes);
      fail("invalid document accepted");
    } catch (IllegalArgumentException | IOException expected) {
      // Both parser and semantic errors reject before returning a validated message.
    }
  }

  @Test
  public void validCorpusMatchesCanonicalBytesAndHashes() throws IOException {
    JsonNode golden = mapper.readTree(fixtures.resolve("golden.json").toFile());
    List<Path> paths = corpus("valid");
    assertEquals(golden.size(), paths.size());
    for (Path path : paths) {
      ProtocolValidator.Message message = validator.validate(Files.readAllBytes(path));
      JsonNode expected = golden.get(path.getFileName().toString().replace(".json", ""));
      assertArrayEquals(expected.get("canonical").asText().getBytes(StandardCharsets.US_ASCII),
          message.canonicalBytes());
      assertEquals(expected.get("sha256").asText(), message.sha256());
    }
  }

  @Test
  public void invalidCorpusRejectsBeforeReturningMessage() throws IOException {
    for (String directory : Arrays.asList("invalid", "parser-invalid")) {
      for (Path path : corpus(directory)) {
        try {
          rejects(Files.readAllBytes(path));
        } catch (AssertionError error) {
          throw new AssertionError(path.toString(), error);
        }
      }
    }
  }

  @Test
  public void validatedResultCannotBeChangedThroughReturnedCopies() throws IOException {
    ProtocolValidator.Message message = valid("run");
    String hash = message.sha256();
    ((ObjectNode) message.json()).put("command", "changed");
    byte[] bytes = message.canonicalBytes();
    Arrays.fill(bytes, (byte) 'x');
    assertEquals(hash, message.sha256());
    assertEquals("run-command-a", message.json().get("command").asText());
  }

  @Test
  public void capabilityRequirementsNeedActualAgentSupport() throws IOException {
    ObjectNode hard = valid("batch").json().deepCopy();
    ((ObjectNode) hard.get("template").get("resources")).put("memoryEnforcement", "hard");
    ((ArrayNode) hard.get("template").get("requiredCapabilities")).add("hard-memory");
    ProtocolValidator.Message message = validator.validate(mapper.writeValueAsBytes(hard));
    try {
      ProtocolValidator.requireCapabilities(message, Collections.emptySet());
      fail("missing hard memory support accepted");
    } catch (IllegalArgumentException expected) { }
    ProtocolValidator.requireCapabilities(message, Collections.singleton("hard-memory"));
  }

  @Test
  public void assignmentMustResolveItsJobAndRevision() throws IOException {
    ProtocolValidator.Message job = valid("service");
    ProtocolValidator.requireResolution(job, valid("service-run-a"));
    ProtocolValidator.requireResolution(job, valid("service-run-b"));
    ObjectNode changed = valid("service-run-a").json().deepCopy();
    ((ArrayNode) changed.get("assignment").get("argv")).set(2, mapper.getNodeFactory().textNode("18082"));
    try {
      ProtocolValidator.requireResolution(job, validator.validate(mapper.writeValueAsBytes(changed)));
      fail("incorrect port resolution accepted");
    } catch (IllegalArgumentException expected) { }
    changed = valid("service-run-a").json().deepCopy();
    changed.put("desiredRevision", "2");
    try {
      ProtocolValidator.requireResolution(job, validator.validate(mapper.writeValueAsBytes(changed)));
      fail("incorrect desired revision accepted");
    } catch (IllegalArgumentException expected) { }
  }

  @Test
  public void authorityRefreshPreservesImmutableBodyHash() throws IOException {
    JsonNode first = valid("delivery").json();
    JsonNode second = valid("delivery-refreshed-authority").json();
    assertEquals(first.get("body"), second.get("body"));
    assertEquals(first.get("bodySha256"), second.get("bodySha256"));
  }

  @Test
  public void lexicalAndResourceLimitsReject() throws IOException {
    rejects(new byte[ProtocolValidator.MAX_BYTES + 1]);
    String deep = String.join("", Collections.nCopies(65, "[")) + "0"
        + String.join("", Collections.nCopies(65, "]"));
    rejects(deep.getBytes(StandardCharsets.US_ASCII));
    byte[] run = valid("run").canonicalBytes();
    rejects((new String(run, StandardCharsets.US_ASCII) + "{}").getBytes(StandardCharsets.US_ASCII));
    rejects(new byte[] {'"', (byte) 0xff, '"'});
    rejects(new String(run, StandardCharsets.US_ASCII).getBytes(StandardCharsets.UTF_16));
    byte[] bom = new byte[run.length + 3];
    bom[0] = (byte) 0xef; bom[1] = (byte) 0xbb; bom[2] = (byte) 0xbf;
    System.arraycopy(run, 0, bom, 3, run.length);
    rejects(bom);
  }
}
