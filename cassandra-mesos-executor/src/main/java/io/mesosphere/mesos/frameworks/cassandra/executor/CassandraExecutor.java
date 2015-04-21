/**
 *    Copyright (C) 2015 Mesosphere, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra.executor;

import ch.qos.logback.classic.LoggerContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.*;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import static io.mesosphere.mesos.frameworks.cassandra.executor.ExecutorUtils.taskStatus;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

    private static final long SHUTDOWN_TIMEOUT = TimeUnit.MINUTES.toMillis(10);

    static {
        // don't let logback load classes ...
        // logback loads classes mentioned in stack traces...
        // this may involve C* node classes - and that may cause strange exceptions hiding the original cause
        ((LoggerContext)LoggerFactory.getILoggerFactory()).setPackagingDataEnabled(false);
    }

    @NotNull
    private final ObjectFactory objectFactory;

    // TODO in order to be able to re-register (restart the executor) we need to switch to PID handling.
    // We could do that using unsafe stuff and construct our own instance of java.lang.UNIXProcess.
    // But without I/O redirection. Note: we need an Oracle JVM then - not a big deal - C* requires an Oracle JVM.
    private volatile WrappedProcess process;
    private volatile JmxConnect jmxConnect;

    private ExecutorInfo executorInfo;
    private TaskInfo serverTask;

    @NotNull
    private final AtomicReference<AbstractNodeJob> currentJob = new AtomicReference<>();

    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final ScheduledExecutorService scheduledExecutorService;

    @NotNull
    private final AtomicBoolean killDaemonSingleton = new AtomicBoolean();
    private ScheduledFuture<?> healthCheckTask;

    public CassandraExecutor(@NotNull final ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;

        this.executorService = new ThreadPoolExecutor(0, 5, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private int seq;

            @Override
            public Thread newThread(@NotNull final Runnable r) {
                return new Thread(r, "cassandra-executor-" + (++seq));
            }
        });

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void registered(final ExecutorDriver driver, final ExecutorInfo executorInfo, final FrameworkInfo frameworkInfo, final SlaveInfo slaveInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("registered(driver : {}, executorInfo : {}, frameworkInfo : {}, slaveInfo : {})", driver, protoToString(executorInfo), protoToString(frameworkInfo), protoToString(slaveInfo));
        }
        this.executorInfo = executorInfo;
    }

    @Override
    public void reregistered(final ExecutorDriver driver, final SlaveInfo slaveInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("reregistered(driver : {}, slaveInfo : {})", driver, protoToString(slaveInfo));
        }
    }

    @Override
    public void disconnected(final ExecutorDriver driver) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("disconnected(driver : {})", driver);
        }
        // TODO implement
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        final Marker taskIdMarker = MarkerFactory.getMarker(task.getTaskId().getValue());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(taskIdMarker, "> launchTask(driver : {}, task : {})", driver, protoToString(task));
        }
        try {
            driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_STARTING));
            final ByteString data = task.getData();
            final TaskDetails taskDetails = TaskDetails.parseFrom(data);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(taskIdMarker, "received taskDetails: {}", protoToString(taskDetails));
            }

            handleProcessNoLongerAlive(driver);

            switch (taskDetails.getType()) {
                case EXECUTOR_METADATA:
                    final ExecutorMetadataTask executorMetadataTask = taskDetails.getExecutorMetadataTask();
                    final ExecutorMetadata slaveMetadata = collectSlaveMetadata(executorMetadataTask);
                    final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.EXECUTOR_METADATA)
                        .setExecutorMetadata(slaveMetadata)
                        .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_RUNNING, details));
                    break;
                case CASSANDRA_SERVER_RUN:
                    safeShutdown();
                    try {
                        final CassandraServerRunTask cassandraServerRunTask = taskDetails.getCassandraServerRunTask();
                        process = objectFactory.launchCassandraNodeTask(taskIdMarker, cassandraServerRunTask);

                        try {
                            Thread.sleep(500);
                            final int exitCode = process.exitValue();
                            process = null;

                            LOGGER.error(taskIdMarker, "Cassandra daemon process exited early with code " + exitCode + " - see logs for details");
                            driver.sendStatusUpdate(ExecutorUtils.slaveErrorDetails(task, "Cassandra daemon process exited early with code " + exitCode + " see logs for details", "-", SlaveErrorDetails.ErrorType.PROCESS_EXITED));
                        } catch (InterruptedException | IllegalThreadStateException e) {
                            // that's what we want (ITSE)
                        }

                        serverTask = task;
                        jmxConnect = objectFactory.newJmxConnect(cassandraServerRunTask.getJmx());
                        startCheckingHealth(driver, jmxConnect, cassandraServerRunTask.getHealthCheckIntervalSeconds());
                        driver.sendStatusUpdate(taskStatus(serverTask, TaskState.TASK_RUNNING,
                            SlaveStatusDetails.newBuilder()
                                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.CASSANDRA_SERVER_RUN)
                                .setCassandraServerRunMetadata(CassandraServerRunMetadata.newBuilder()
                                    .setPid(process.getPid()))
                                .build()));
                    } catch (final LaunchNodeException e) {
                        LOGGER.error(taskIdMarker, "Failed to start Cassandra daemon", e);
                        driver.sendStatusUpdate(ExecutorUtils.slaveErrorDetails(task, "Failed to start Cassandra daemon", e.toString(), SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING));
                    }
                    break;
                case UPDATE_CONFIG:
                    if (serverTask == null) {
                        driver.sendStatusUpdate(ExecutorUtils.slaveErrorDetails(task, "Failed to update config - no Cassandra daemon running", "-", SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING));
                    } else {
                        driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_RUNNING));
                        final TaskDetails serverTaskDetails = TaskDetails.parseFrom(serverTask.getData());
                        try {
                            objectFactory.updateCassandraServerConfig(taskIdMarker, serverTaskDetails.getCassandraServerRunTask(), taskDetails.getUpdateConfigTask());
                        } catch (ConfigChangeException e) {
                            driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FAILED));
                        }
                        driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED));
                    }
                    break;
                case NODE_JOB:
                    startJob(driver, task, taskDetails);
                    break;
            }
        } catch (final InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task data to type: " + TaskDetails.class.getName();
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                ExecutorUtils.slaveErrorDetails(task, msg, null, SlaveErrorDetails.ErrorType.PROTOCOL_VIOLATION);
            driver.sendStatusUpdate(taskStatus);
        } catch (final Exception e) {
            final String msg = "Error starting task due to exception.";
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                ExecutorUtils.slaveErrorDetails(task, msg, e.getMessage() != null ? e.getMessage() : "-", SlaveErrorDetails.ErrorType.TASK_START_FAILURE);
            driver.sendStatusUpdate(taskStatus);
        }
        LOGGER.debug(taskIdMarker, "< launchTask(driver : {}, task : {})", driver, protoToString(task));
    }

    private void startJob(@NotNull final ExecutorDriver driver, @NotNull final TaskInfo task, @NotNull final TaskDetails taskDetails) {
        if (process == null) {
            ExecutorUtils.serverProcessNotRunning(driver, task);
            return;
        }

        final NodeJobTask nodeJob = taskDetails.getNodeJobTask();
        final AbstractNodeJob job;
        switch (nodeJob.getJobType()) {
            case REPAIR:
                job = new NodeRepairJob(task.getTaskId());
                break;
            case CLEANUP:
                job = new NodeCleanupJob(task.getTaskId(), executorService);
                break;
            default:
                return;
        }
        final TaskStatus taskStatus = startJob(task, job);
        driver.sendStatusUpdate(taskStatus);
    }

    private void jobStatus(@NotNull final ExecutorDriver driver, @Nullable final TaskInfo task /*TODO: This is only ever called with null.*/) {
        if (process == null) {
            LOGGER.error("No Cassandra process to handle node-job-status");
            if (task != null) {
                ExecutorUtils.serverProcessNotRunning(driver, task);
            }
            return;
        }

        final AbstractNodeJob current = currentJob.get();
        if (current == null) {
            LOGGER.error("No current job to handle node-job-status");
            return;
        }

        final NodeJobStatus.Builder status = NodeJobStatus.newBuilder()
                .setExecutorId(this.executorInfo.getExecutorId().getValue())
                .setTaskId(current.getTaskId().getValue())
                .setJobType(current.getType())
                .setRunning(!current.isFinished())
                .addAllRemainingKeyspaces(current.getRemainingKeyspaces())
                .addAllProcessedKeyspaces(current.getKeyspaceStatus().values())
                .setStartedTimestamp(current.getStartTimestamp())
                .setFinishedTimestamp(current.getFinishedTimestamp());
        final SlaveStatusDetails repairDetails = SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(status).build();
        LOGGER.debug("Sending node-job-status result, finished={}", current.isFinished());
        driver.sendFrameworkMessage(repairDetails.toByteArray());
        if (current.isFinished()) {
            driver.sendStatusUpdate(taskStatus(executorInfo.getExecutorId(), current.getTaskId(), TaskState.TASK_FINISHED, repairDetails));
            currentJob.set(null);
        }
        if (task != null) {
            driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED, repairDetails));
        }
    }

    @Override
    public void killTask(final ExecutorDriver driver, final TaskID taskId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("killTask(driver : {}, taskId : {})", driver, protoToString(taskId));
        }

        if (serverTask != null && serverTask.getTaskId().equals(taskId)) {
            killCassandraDaemon(driver);
        }
        else if (executorInfo.getExecutorId().getValue().equals(taskId.getValue())) {
            killCassandraDaemon(driver);

            driver.sendStatusUpdate(taskStatus(executorInfo.getExecutorId(), taskId, TaskState.TASK_FINISHED, ExecutorUtils.nullSlaveStatusDetails()));
            driver.stop();
        }
    }

    @Override
    public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frameworkMessage(driver : {}, data : {})", driver, data);
        }

        try {
            final TaskDetails taskDetails = TaskDetails.parseFrom(data);

            handleProcessNoLongerAlive(driver);

            switch (taskDetails.getType()) {
                case NODE_JOB_STATUS:
                    jobStatus(driver, null);
                    break;
                default:
                    LOGGER.debug("Unhandled frameworkMessage with detail type: {}", taskDetails.getType());
                    break;
            }
        } catch (final Exception e) {
            final String msg = "Error handling framework message due to exception.";
            LOGGER.error(msg, e);
        }
    }

    @Override
    public void shutdown(final ExecutorDriver driver) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("shutdown(driver : {})", driver);
        }

        // TODO implement

        executorService.shutdown();
    }

    @Override
    public void error(final ExecutorDriver driver, final String message) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("error(driver : {}, message : {})", driver, message);
        }
        // TODO implement
    }

    // Jobs

    // exposed for unit tests
    AbstractNodeJob getCurrentJob() {
        return currentJob.get();
    }

    @NotNull
    public TaskStatus startJob(@NotNull final TaskInfo task, @NotNull final AbstractNodeJob nodeJob) {
        final AbstractNodeJob current = currentJob.get();
        if (current == null || current.isFinished()) {
            currentJob.set(nodeJob);
            if (nodeJob.start(jmxConnect)) {
                LOGGER.info("Starting node job {}", nodeJob.getType());
                nodeJob.startNextKeyspace();
                return taskStatus(task, TaskState.TASK_RUNNING, ExecutorUtils.nullSlaveStatusDetails());
            }
            else {
                LOGGER.error("Failed to start node job {}", nodeJob.getType());
                nodeJob.close();
                currentJob.set(null);
            }
        }
        else {
            LOGGER.error("Cannot start new node job {} while another node job is not finished ({})",
                nodeJob.getType(), current.getType());
        }
        return ExecutorUtils.slaveErrorDetails(task, "Another node job is still running", null, SlaveErrorDetails.ErrorType.ANOTHER_JOB_RUNNING);
    }

    private void forceCurrentJobAbort() {
        final AbstractNodeJob current = currentJob.getAndSet(null);
        if (current != null) {
            current.forceAbort();
        }
    }

    //

    private void killCassandraDaemon(@NotNull final ExecutorDriver driver) {
        if (!killDaemonSingleton.compareAndSet(false, true))
            return;
        // do this asynchronously to let the executor stay responsive to the executor-driver
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final TaskInfo task = serverTask;

                    if (process != null) {
                        LOGGER.info("Stopping Cassandra Daemon...");
                        try {
                            try {
                                // note: although we could also use jmxConnect.getStorageServiceProxy().stopDaemon();
                                // it is safe to do it this way
                                process.destroy();
                            } catch (final Throwable ignored) {
                                // any kind of strange exception may occur since shutdown closes everything
                            }
                            // TODO make shutdown timeout configurable
                            final long timeoutAt = System.currentTimeMillis() + SHUTDOWN_TIMEOUT;
                            while (true) {
                                if (timeoutAt < System.currentTimeMillis()) {
                                    process.destroyForcibly();
                                    break;
                                }
                                try {
                                    final int exitCode = process.exitValue();
                                    LOGGER.info("Cassandra process terminated with exit code {}", exitCode);
                                    break;
                                } catch (final IllegalThreadStateException e) {
                                    // ignore
                                }
                            }
                        } catch (final Throwable e) {
                            LOGGER.error("Failed to stop Cassandra daemon - forcing process destroy", e);
                            try {
                                process.destroy();
                            } catch (final Throwable ignored) {
                                // any kind of strange exception may occur since shutdown closes everything
                            }
                        } finally {
                            process = null;
                        }

                    }
                    closeJmx();

                    stopCheckingHealth();

                    serverTask = null;

                    if (task != null) {
                        LOGGER.debug("Sending status update to scheduler that Cassandra server task has finished");
                        driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED));
                    }

                    LOGGER.info("Cassandra process terminated");
                } finally {
                    killDaemonSingleton.set(false);
                }
            }
        });
    }

    @NotNull
    private static ExecutorMetadata collectSlaveMetadata(@NotNull final ExecutorMetadataTask executorMetadata) {
        return ExecutorMetadata.newBuilder()
            .setExecutorId(executorMetadata.getExecutorId())
            .setIp(executorMetadata.getIp())
            .setWorkdir(System.getProperty("user.dir"))
            .build();
    }

    private void safeShutdown() {
        closeJmx();
        killProcess();
        serverTask = null;
    }

    private void killProcess() {
        if (process != null) {
            try {
                // this is only a kind of "last resort" - usual shutdown is performed in killCassandraDaemon() method
                process.destroyForcibly();
            } catch (final Exception ignores) {
                // ignore this
            }
            process = null;

            stopCheckingHealth();
        }
    }

    private void closeJmx() {
        if (jmxConnect != null) {
            // close JmxConnect asynchronously because it may take long (*not* funny)
            final JmxConnect jc = jmxConnect;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        LOGGER.debug("Closing JMX connection to Cassandra process");
                        jc.close();
                    } catch (final Exception ignores) {
                        // ignore this
                    }
                }
            });

            jmxConnect = null;
        }
    }


    /**
     * Handles the state when the Cassandra server process exited on its own and cleans up our state.
     */
    private void handleProcessNoLongerAlive(@NotNull final ExecutorDriver driver) {
        if (process == null)
            return;
        try {
            final int exitCode = process.exitValue();
            LOGGER.error("Cassandra process terminated unexpectedly with exit code {}", exitCode);

            if (serverTask != null) {
                final TaskStatus taskStatus =
                    ExecutorUtils.slaveErrorDetails(serverTask, "process exited with exit code " + exitCode, null, SlaveErrorDetails.ErrorType.PROCESS_EXITED);
                driver.sendStatusUpdate(taskStatus);
            }

            safeShutdown();

            forceCurrentJobAbort();

            stopCheckingHealth();

        } catch (final IllegalThreadStateException e) {
            // ignore
        }
    }

    private void startCheckingHealth(@NotNull final ExecutorDriver driver, @Nullable final JmxConnect jmxConnect, final long intervalSeconds) {
        stopCheckingHealth();
        LOGGER.debug("Scheduling background health check task to run every {} seconds", intervalSeconds);
        healthCheckTask = scheduledExecutorService.scheduleAtFixedRate(
            new ServerHealthCheckTask(driver, jmxConnect),
            0,
            intervalSeconds,
            TimeUnit.SECONDS
        );
    }

    private void stopCheckingHealth() {
        if (healthCheckTask != null) {
            LOGGER.debug("Stopping scheduled background health check task");
            healthCheckTask.cancel(true);
            healthCheckTask = null;
        }
    }

    public static void main(final String[] args) {
        final MesosExecutorDriver driver = new MesosExecutorDriver(new CassandraExecutor(new ProdObjectFactory()));
        final int status;
        switch (driver.run()) {
            case DRIVER_STOPPED:
                status = 0;
                break;
            case DRIVER_ABORTED:
                status = 1;
                break;
            case DRIVER_NOT_STARTED:
                status = 2;
                break;
            default:
                status = 3;
                break;
        }
        driver.stop();

        System.exit(status);
    }

}
