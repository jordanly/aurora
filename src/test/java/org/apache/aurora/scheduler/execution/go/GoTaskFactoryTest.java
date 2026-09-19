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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.PartitionPolicy;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.execution.ExecutionOffer;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class GoTaskFactoryTest extends EasyMockTest {
  private static final String PROCESS = "{\"version\":\"aurora-process-v1\","
      + "\"argv\":[\"/bin/echo\",\"hello\"],\"env\":{\"MODE\":\"test\"},"
      + "\"graceMillis\":1000}";

  @Test
  public void malformedProfileErrorsDoNotExposeExecutorData() {
    control.replay();
    TaskDescriptionException malformed = assertThrows(TaskDescriptionException.class,
        () -> factory().validate(task("{\"version\":synthetic-profile-secret}")));
    assertFalse(malformed.toString().contains("synthetic-profile-secret"));
    assertNull(malformed.getCause());
    TaskDescriptionException unknownField = assertThrows(TaskDescriptionException.class,
        () -> factory().validate(task(PROCESS.replace("\"graceMillis\":1000",
            "\"graceMillis\":1000,\"synthetic-profile-secret\":1"))));
    assertFalse(unknownField.toString().contains("synthetic-profile-secret"));
    assertFalse(unknownField.getCause().toString().contains("synthetic-profile-secret"));
  }

  @Test
  public void processProfileOwnsCollectionsAndRedactsValues() {
    control.replay();
    var argv = new java.util.ArrayList<>(List.of("/bin/echo", "synthetic-profile-secret"));
    var env = new java.util.HashMap<>(Map.of("TOKEN", "synthetic-profile-secret"));
    var profile = new GoTaskFactory.ProcessProfile(argv, env, 1000, Optional.empty());
    argv.clear();
    env.clear();
    assertEquals(2, profile.argv().size());
    assertEquals(1, profile.env().size());
    assertThrows(UnsupportedOperationException.class, () -> profile.argv().clear());
    assertThrows(UnsupportedOperationException.class, () -> profile.env().clear());
    assertFalse(profile.toString().contains("synthetic-profile-secret"));
  }

  @Test
  public void validatesSupportedProcessProfile() throws Exception {
    control.replay();
    factory().validate(task(PROCESS));
  }

  @Test
  public void rejectsUnsupportedProcessFieldsAndResources() {
    control.replay();
    assertRejected(PROCESS.replace("\"graceMillis\":1000", "\"graceMillis\":1000,\"extra\":1"));
    assertRejected(PROCESS.replace("/bin/echo", "echo"));
    assertRejected(PROCESS.replace("\"env\":{\"MODE\":\"test\"}", "\"env\":[]"));

    ITaskConfig docker = task(
        PROCESS, Container.docker(new org.apache.aurora.gen.DockerContainer("image")));
    assertThrowsTaskDescription(() -> factory().validate(docker));
    ITaskConfig thermos = ITaskConfig.build(task(PROCESS).newBuilder()
        .setExecutorConfig(new ExecutorConfig(apiConstants.AURORA_EXECUTOR_NAME, PROCESS)));
    assertThrowsTaskDescription(() -> factory().validate(thermos));
    ITaskConfig revocable = ITaskConfig.build(task(PROCESS).newBuilder()
        .setTier(TaskTestUtil.REVOCABLE_TIER_NAME));
    assertThrowsTaskDescription(() -> factory().validate(revocable));
    ITaskConfig partitioned = ITaskConfig.build(task(PROCESS).newBuilder()
        .setPartitionPolicy(new PartitionPolicy().setReschedule(true)));
    assertThrowsTaskDescription(() -> factory().validate(partitioned));
    ITaskConfig ports = ITaskConfig.build(task(PROCESS).newBuilder()
        .setResources(ImmutableSet.of(numCpus(1), ramMb(1), diskMb(1),
            org.apache.aurora.gen.Resource.namedPort("http"))));
    assertThrowsTaskDescription(() -> factory().validate(ports));
  }

  @Test
  public void rejectsFractionalCpuMillisAndUnrepresentableJobKeys() throws Exception {
    control.replay();
    var valid = task(PROCESS).newBuilder()
        .setResources(ImmutableSet.of(numCpus(0.6), ramMb(1), diskMb(1)));
    factory().validate(ITaskConfig.build(valid));
    for (double cores : List.of(1.001, 1.007, 4.097, Integer.MAX_VALUE / 1000.0)) {
      factory().validate(ITaskConfig.build(valid.deepCopy()
          .setResources(ImmutableSet.of(numCpus(cores), ramMb(1), diskMb(1)))));
    }
    var fractional = valid.deepCopy()
        .setResources(ImmutableSet.of(numCpus(0.6001), ramMb(1), diskMb(1)));
    assertThrowsTaskDescription(() -> factory().validate(ITaskConfig.build(fractional)));
    var invalidKey = valid.deepCopy();
    invalidKey.getJob().setName("unsupported_name");
    assertThrowsTaskDescription(() -> factory().validate(ITaskConfig.build(invalidKey)));
  }

  @Test
  public void identityIsStableAndPreparedRunHasInteropShape() throws Exception {
    GoAgentConfig config = new GoAgentConfig(
        "cluster", "incarnation", null, null, "", null, "",
        List.of(new GoAgentConfig.Node(
            "agent-1", URI.create("https://agent-1"), "journal", "boot", "runtime",
            1000, 1024, 1)));
    GoTaskFactory factory = new GoTaskFactory(config, TaskTestUtil.TIER_MANAGER);
    ExecutionOffer offer = createMock(ExecutionOffer.class);
    org.easymock.EasyMock.expect(offer.getAgentId()).andReturn("agent-1");
    control.replay();

    GoTaskFactory.Launch launch = (GoTaskFactory.Launch) factory.prepare(
        IAssignedTask.build(new AssignedTask()
            .setTaskId("task-1")
            .setInstanceId(3)
            .setTask(task(PROCESS).newBuilder()
                .setResources(ImmutableSet.of(numCpus(1.001), ramMb(1), diskMb(1))))),
        offer, false);
    var run = WireJson.parse(launch.body().getBytes(StandardCharsets.US_ASCII));

    assertEquals("task-1", launch.taskId());
    assertEquals("agent-1", launch.agentId());
    assertEquals("native-v1alpha1", run.get("version").asText());
    assertEquals("Run", run.get("kind").asText());
    assertEquals("agent-1", run.get("target").get("node").asText());
    assertEquals("main", run.get("assignment").get("process").asText());
    assertEquals(1001, run.path("assignment").path("resources").path("cpuMillis").asInt());
    assertEquals("1", run.get("desiredRevision").asText());
    // Frozen canonical assignment digest from the original launch shape, before profile extraction.
    assertEquals("5f824cbe66bbbde547196f8de88e0dda8ae12a86a0530623b31df03f87169911",
        run.path("templateSha256").asText());
    assertNotEquals(
        GoTaskFactory.identity("a-", "task-1"),
        GoTaskFactory.identity("a-", "task-2"));
  }

  @Test
  public void validatesBoundedOptionalHealth() throws Exception {
    control.replay();
    String health = "{\"kind\":\"tcp\",\"port\":8080,\"network\":\"agent-container\","
        + "\"intervalMillis\":1000,\"timeoutMillis\":100,"
        + "\"startupTimeoutMillis\":30000,\"failureThreshold\":3}";
    String data = PROCESS.substring(0, PROCESS.length() - 1) + ",\"health\":" + health + "}";
    factory().validate(task(data));
    assertEquals(Optional.of("agent-container:8080"), GoTaskFactory.healthSocket(task(data)));
    assertEquals(Optional.empty(), GoTaskFactory.healthSocket(task(PROCESS)));
    for (String invalid : List.of(
        data.replace("8080", "0"), data.replace("8080", "65536"),
        data.replace("\"tcp\"", "\"http\""),
        data.replace("\"timeoutMillis\":100", "\"timeoutMillis\":251"),
        data.replace("\"startupTimeoutMillis\":30000", "\"startupTimeoutMillis\":600001"),
        data.replace("\"failureThreshold\":3", "\"failureThreshold\":0"),
        data.replace(",\"failureThreshold\":3", ""))) {
      assertRejected(invalid);
    }
  }

  private static GoTaskFactory factory() {
    return new GoTaskFactory(null, TaskTestUtil.TIER_MANAGER);
  }

  private static ITaskConfig task(String process) {
    return task(process, Container.mesos(new MesosContainer()));
  }

  private static ITaskConfig task(String process, Container container) {
    return ITaskConfig.build(TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder()
        .setExecutorConfig(new ExecutorConfig(GoTaskFactory.EXECUTOR, process))
        .setContainer(container)
        .setMesosFetcherUris(ImmutableSet.of())
        .setPartitionPolicy(new PartitionPolicy().setReschedule(false))
        .setResources(ImmutableSet.of(numCpus(1), ramMb(1), diskMb(1))));
  }

  private static void assertRejected(String process) {
    assertThrowsTaskDescription(() -> factory().validate(task(process)));
  }

  private static void assertThrowsTaskDescription(ThrowingAction action) {
    try {
      action.run();
      fail("Expected TaskDescriptionException");
    } catch (TaskDescriptionException expected) {
      // Expected.
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @FunctionalInterface
  private interface ThrowingAction {
    void run() throws Exception;
  }
}
