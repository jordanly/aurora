package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.twitter.common.base.Closure;
import com.twitter.common.util.BuildInfo;
import com.twitter.mesos.Message;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.executor.Task.TaskRunException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ExecutorStatus;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import com.twitter.mesos.scheduler.ExecutorRootDir;
import org.apache.commons.io.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;

/**
 * ExecutorCore
 *
 * TODO(wfarner): When a LiveTask is terminated, should we replace its entry with a DeadTask?
 *
 * @author wfarner
 */
public class ExecutorCore implements TaskManager {
  private static final Logger LOG = Logger.getLogger(MesosExecutorImpl.class.getName());

  /**
   * {@literal @Named} binding key for the task executor service.
   */
  public static final String TASK_EXECUTOR =
      "com.twitter.mesos.executor.ExecutorCore.TASK_EXECUTOR";

  private final Map<Integer, Task> tasks = Maps.newConcurrentMap();

  private final File executorRootDir;

  private final ScheduledExecutorService syncExecutor = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutorSync-%d").build());

  private final AtomicReference<String> slaveId = new AtomicReference<String>();

  private final BuildInfo buildInfo;
  private final Function<AssignedTask, Task> taskFactory;
  private final ExecutorService taskExecutor;
  private final Function<Message, Integer> messageHandler;

  @Inject
  public ExecutorCore(@ExecutorRootDir File executorRootDir, BuildInfo buildInfo,
      Function<AssignedTask, Task> taskFactory,
      @Named(TASK_EXECUTOR) ExecutorService taskExecutor,
      Function<Message, Integer> messageHandler) {
    this.executorRootDir = Preconditions.checkNotNull(executorRootDir);
    this.buildInfo = Preconditions.checkNotNull(buildInfo);
    this.taskFactory = Preconditions.checkNotNull(taskFactory);
    this.taskExecutor = Preconditions.checkNotNull(taskExecutor);
    this.messageHandler = Preconditions.checkNotNull(messageHandler);
  }

  /**
   * Adds dead tasks that the executor may report record of.
   *
   * @param deadTasks Dead tasks to store.
   */
  void addDeadTasks(Iterable<Task> deadTasks) {
    Preconditions.checkNotNull(deadTasks);

    for (Task task : deadTasks) {
      tasks.put(task.getId(), task);
    }
  }

  /**
   * Initiates periodic tasks that the executor performs (state sync, resource monitoring, etc).
   */
  void startPeriodicTasks() {
    // TODO(wfarner): Apply a shutdown registry here to cleanly halt these tasks.
    new ResourceManager(this, executorRootDir).start();
    startStateSync();
    startRegisteredTaskPusher();
  }

  void setSlaveId(String slaveId) {
    this.slaveId.set(Preconditions.checkNotNull(slaveId));
  }

  /**
   * Executes a task on the system.
   *
   * @param assignedTask The assigned task to run.
   * @param completedCallback The callback to call when the task has completed, will not be called
   *    if the task failed to start.
   * @throws TaskRunException If the task failed to start.
   */
  public void executeTask(final AssignedTask assignedTask,
      final Closure<ScheduleStatus> completedCallback) throws TaskRunException {
    Preconditions.checkNotNull(assignedTask);

    final int taskId = assignedTask.getTaskId();

    LOG.info(String.format("Received task for execution: %s - %d",
        Tasks.jobKey(assignedTask), taskId));

    final Task task = taskFactory.apply(assignedTask);

    tasks.put(taskId, task);

    try {
      task.stage();
      task.run();
    } catch (TaskRunException e) {
      LOG.log(Level.SEVERE, "Failed to stage task " + taskId, e);
      task.terminate(FAILED);
      deleteCompletedTask(taskId);
      throw e;
    }

    taskExecutor.execute(new Runnable() {
      @Override public void run() {
        LOG.info("Waiting for task " + taskId + " to complete.");
        ScheduleStatus state = task.blockUntilTerminated();
        LOG.info("Task " + taskId + " completed in state " + state);
        completedCallback.execute(state);
      }
    });
  }

  public void stopLiveTask(int taskId) {
    Task task = tasks.get(taskId);

    if (task != null && task.isRunning()) {
      LOG.info("Killing task: " + task);
      task.terminate(ScheduleStatus.KILLED);
    } else if (task == null) {
      LOG.severe("No such task found: " + taskId);
    }
  }

  public Task getTask(int taskId) {
    return tasks.get(taskId);
  }

  public Iterable<Task> getTasks() {
    return Iterables.unmodifiableIterable(tasks.values());
  }

  @Override
  public Iterable<Task> getLiveTasks() {
    return Iterables.unmodifiableIterable(Iterables.filter(tasks.values(),
        new Predicate<Task>() {
          @Override public boolean apply(Task task) {
            return task.isRunning();
          }
        }));
  }

  @Override
  public boolean hasTask(int taskId) {
    return tasks.containsKey(taskId);
  }

  @Override
  public boolean isRunning(int taskId) {
    return hasTask(taskId) && tasks.get(taskId).isRunning();
  }

  @Override
  public void deleteCompletedTask(int taskId) {
    Preconditions.checkState(!isRunning(taskId), "Task " + taskId + " is still running!");
    tasks.remove(taskId);
  }

  /**
   * Cleanly shuts down the executor.
   *
   * @return The active tasks that were killed as the executor shut down.
   */
  public Iterable<Task> shutdownCore() {
    LOG.info("Shutting down executor core.");
    Iterable<Task> liveTasks = getLiveTasks();
    for (Task task : liveTasks) {
      stopLiveTask(task.getId());
    }

    return liveTasks;
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Failed to look up own hostname.", e);
      return null;
    }
  }

  private void startStateSync() {
    final Properties buildProperties = buildInfo.getProperties();

    final String DEFAULT = "unknown";
    final ExecutorStatus baseStatus = new ExecutorStatus()
        .setHost(getHostName())
        .setBuildUser(buildProperties.getProperty(BuildInfo.Key.USER.value, DEFAULT))
        .setBuildMachine(buildProperties.getProperty(BuildInfo.Key.MACHINE.value, DEFAULT))
        .setBuildPath(buildProperties.getProperty(BuildInfo.Key.PATH.value, DEFAULT))
        .setBuildGitTag(buildProperties.getProperty(BuildInfo.Key.GIT_TAG.value, DEFAULT))
        .setBuildGitRevision(buildProperties.getProperty(BuildInfo.Key.GIT_REVISION.value, DEFAULT))
        .setBuildTimestamp(buildProperties.getProperty(BuildInfo.Key.TIMESTAMP.value, DEFAULT));

    Runnable syncer = new Runnable() {
      @Override public void run() {
        if (slaveId.get() == null) return;

        ExecutorStatus status = new ExecutorStatus(baseStatus)
            .setSlaveId(slaveId.get());

        try {
          status.setDiskFreeKb(FileSystemUtils.freeSpaceKb(executorRootDir.getAbsolutePath()));
        } catch (IOException e) {
          LOG.log(Level.INFO, "Failed to get disk free space.", e);
        }

        SchedulerMessage message = new SchedulerMessage();
        message.setExecutorStatus(status);
        messageHandler.apply(new Message(message));
      }
    };

    // TODO(wfarner): Make sync interval configurable.
    syncExecutor.scheduleAtFixedRate(syncer, 30, 30, TimeUnit.SECONDS);
  }

  private void startRegisteredTaskPusher() {
    Runnable pusher = new Runnable() {
      @Override public void run() {
        RegisteredTaskUpdate update = new RegisteredTaskUpdate().setSlaveHost(getHostName());

        for (Map.Entry<Integer, Task> entry : tasks.entrySet()) {
          Task task = entry.getValue();

          update.addToTaskInfos(new LiveTaskInfo()
              .setTaskId(entry.getKey())
              .setTaskInfo(task.getAssignedTask().getTask())
              .setResources(task.getResourceConsumption())
              .setStatus(task.getScheduleStatus()));
        }

        SchedulerMessage message = new SchedulerMessage();
        message.setTaskUpdate(update);
        messageHandler.apply(new Message(message));
      }
    };

    // TODO(wfarner): Make push interval configurable.
    syncExecutor.scheduleAtFixedRate(pusher, 5, 5, TimeUnit.SECONDS);
  }
}
