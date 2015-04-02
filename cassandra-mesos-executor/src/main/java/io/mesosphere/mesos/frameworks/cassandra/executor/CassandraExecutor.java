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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
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

    private final ObjectFactory objectFactory;

    // TODO in order to be able to re-register (restart the executor) we need to switch to PID handling.
    // We could do that using unsafe stuff and construct our own instance of java.lang.UNIXProcess.
    // But without I/O redirection. Note: we need an Oracle JVM then - not a big deal - C* requires an Oracle JVM.
    private volatile WrappedProcess process;
    private volatile JmxConnect jmxConnect;

    private ExecutorInfo executorInfo;
    private TaskInfo serverTask;

    private final AtomicReference<AbstractNodeJob> currentJob = new AtomicReference<>();

    private final ExecutorService executorService;

    private final AtomicBoolean killDaemonSingleton = new AtomicBoolean();

    public CassandraExecutor(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;

        this.executorService = new ThreadPoolExecutor(0, 5, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private int seq;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "cassandra-executor-" + (++seq));
            }
        });
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
                    SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.EXECUTOR_METADATA)
                        .setExecutorMetadata(slaveMetadata)
                        .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_RUNNING, details));
                    break;
                case CASSANDRA_SERVER_RUN:
                    safeShutdown(driver);
                    try {
                        process = objectFactory.launchCassandraNodeTask(taskIdMarker, taskDetails.getCassandraServerRunTask());

                        try {
                            Thread.sleep(500);
                            int exitCode = process.exitValue();
                            process = null;

                            LOGGER.error(taskIdMarker, "Cassandra daemon process exited early with code " + exitCode + " - see logs for details");
                            driver.sendStatusUpdate(slaveErrorDetails(task, "Cassandra daemon process exited early with code " + exitCode + " see logs for details", "-", SlaveErrorDetails.ErrorType.PROCESS_EXITED));
                        } catch (InterruptedException | IllegalThreadStateException e) {
                            // that's what we want (ITSE)
                        }

                        serverTask = task;
                        jmxConnect = objectFactory.newJmxConnect(taskDetails.getCassandraServerRunTask().getJmx());
                        driver.sendStatusUpdate(taskStatus(serverTask, TaskState.TASK_RUNNING,
                            SlaveStatusDetails.newBuilder()
                                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.CASSANDRA_SERVER_RUN)
                                .setCassandraServerRunMetadata(CassandraServerRunMetadata.newBuilder()
                                    .setPid(process.getPid()))
                                .build()));
                    } catch (LaunchNodeException e) {
                        LOGGER.error(taskIdMarker, "Failed to start Cassandra daemon", e);
                        driver.sendStatusUpdate(slaveErrorDetails(task, "Failed to start Cassandra daemon", e.toString(), SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING));
                    }
                    break;
                case UPDATE_CONFIG:
                    if (serverTask == null) {
                        driver.sendStatusUpdate(slaveErrorDetails(task, "Failed to update config - no Cassandra daemon running", "-", SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING));
                    } else {
                        TaskDetails serverTaskDetails = TaskDetails.parseFrom(serverTask.getData());
                        objectFactory.updateCassandraServerConfig(taskIdMarker, serverTaskDetails.getCassandraServerRunTask(), taskDetails.getUpdateConfigTask());
                    }
                    break;
                case NODE_JOB:
                    startJob(driver, task, taskDetails);
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task data to type: " + TaskDetails.class.getName();
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                slaveErrorDetails(task, msg, null, SlaveErrorDetails.ErrorType.PROTOCOL_VIOLATION);
            driver.sendStatusUpdate(taskStatus);
        } catch (Exception e) {
            final String msg = "Error starting task due to exception.";
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                slaveErrorDetails(task, msg, e.getMessage() != null ? e.getMessage() : "-", SlaveErrorDetails.ErrorType.TASK_START_FAILURE);
            driver.sendStatusUpdate(taskStatus);
        }
        LOGGER.debug(taskIdMarker, "< launchTask(driver : {}, task : {})", driver, protoToString(task));
    }

    private void startJob(ExecutorDriver driver, TaskInfo task, TaskDetails taskDetails) {
        if (process == null) {
            serverProcessNotRunning(driver, task);
            return;
        }

        NodeJobTask nodeJob = taskDetails.getNodeJobTask();
        AbstractNodeJob job;
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
        TaskStatus taskStatus = startJob(task, job);
        driver.sendStatusUpdate(taskStatus);
    }

    private void jobStatus(ExecutorDriver driver, TaskInfo task) {
        if (process == null) {
            LOGGER.error("No Cassandra process to handle node-job-status");
            if (task != null) {
                serverProcessNotRunning(driver, task);
            }
            return;
        }

        AbstractNodeJob current = currentJob.get();
        if (current == null) {
            LOGGER.error("No current job to handle node-job-status");
            return;
        }

        NodeJobStatus.Builder status = NodeJobStatus.newBuilder()
                .setExecutorId(this.executorInfo.getExecutorId().getValue())
                .setTaskId(current.getTaskId().getValue())
                .setJobType(current.getType())
                .setRunning(!current.isFinished())
                .addAllRemainingKeyspaces(current.getRemainingKeyspaces())
                .addAllProcessedKeyspaces(current.getKeyspaceStatus().values())
                .setStartedTimestamp(current.getStartTimestamp())
                .setFinishedTimestamp(current.getFinishedTimestamp());
        SlaveStatusDetails repairDetails = SlaveStatusDetails.newBuilder()
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

            driver.sendStatusUpdate(taskStatus(executorInfo.getExecutorId(), taskId, TaskState.TASK_FINISHED, nullSlaveStatusDetails()));
            driver.stop();
        }
    }

    @Override
    public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frameworkMessage(driver : {}, data : {})", driver, data);
        }

        try {
            TaskDetails taskDetails = TaskDetails.parseFrom(data);

            handleProcessNoLongerAlive(driver);

            switch (taskDetails.getType()) {
                case HEALTH_CHECK:
                    LOGGER.info("Received healthCheckTask");
                    healthCheck(driver);
                    break;
                case NODE_JOB_STATUS:
                    jobStatus(driver, null);
                    break;
            }
        } catch (Exception e) {
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

    public TaskStatus startJob(TaskInfo task, AbstractNodeJob nodeJob) {
        AbstractNodeJob current = currentJob.get();
        if (current == null || current.isFinished()) {
            currentJob.set(nodeJob);
            if (nodeJob.start(jmxConnect)) {
                LOGGER.info("Starting node job {}", nodeJob.getType());
                nodeJob.startNextKeyspace();
                return taskStatus(task, TaskState.TASK_RUNNING, nullSlaveStatusDetails());
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
        return slaveErrorDetails(task, "Another node job is still running", null, SlaveErrorDetails.ErrorType.ANOTHER_JOB_RUNNING);
    }

    private void forceCurrentJobAbort() {
        AbstractNodeJob current = currentJob.getAndSet(null);
        if (current != null) {
            current.forceAbort();
        }
    }

    //

    private void killCassandraDaemon(final ExecutorDriver driver) {
        if (!killDaemonSingleton.compareAndSet(false, true))
            return;
        // do this asynchronously to let the executor stay responsive to the executor-driver
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    TaskInfo task = serverTask;

                    if (process != null) {
                        LOGGER.info("Stopping Cassandra Daemon...");
                        try {
                            try {
                                // note: although we could also use jmxConnect.getStorageServiceProxy().stopDaemon();
                                // it is safe to do it this way
                                process.destroy();
                            } catch (Throwable ignored) {
                                // any kind of strange exception may occur since shutdown closes everything
                            }
                            // TODO make shutdown timeout configurable
                            long timeoutAt = System.currentTimeMillis() + SHUTDOWN_TIMEOUT;
                            while (true) {
                                if (timeoutAt < System.currentTimeMillis()) {
                                    process.destroyForcibly();
                                    break;
                                }
                                try {
                                    int exitCode = process.exitValue();
                                    LOGGER.info("Cassandra process terminated with exit code {}", exitCode);
                                    break;
                                } catch (IllegalThreadStateException e) {
                                    // ignore
                                }
                            }
                        } catch (Throwable e) {
                            LOGGER.error("Failed to stop Cassandra daemon - forcing process destroy", e);
                            try {
                                process.destroy();
                            } catch (Throwable ignored) {
                                // any kind of strange exception may occur since shutdown closes everything
                            }
                        } finally {
                            process = null;
                        }

                    }
                    closeJmx();
                    // send health check that process is no longer alive
                    healthCheck(driver);

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

    private void healthCheck(ExecutorDriver driver) {
        final HealthCheckDetails healthCheck = performHealthCheck();
        final SlaveStatusDetails healthCheckDetails = SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
            .setHealthCheckDetails(healthCheck)
            .build();
        driver.sendFrameworkMessage(healthCheckDetails.toByteArray());
    }

    @NotNull
    private static ExecutorMetadata collectSlaveMetadata(@NotNull final ExecutorMetadataTask executorMetadata) {
        return ExecutorMetadata.newBuilder()
            .setExecutorId(executorMetadata.getExecutorId())
            .setIp(executorMetadata.getIp())
            .setWorkdir(System.getProperty("user.dir"))
            .build();
    }

    private void safeShutdown(ExecutorDriver driver) {
        closeJmx();
        killProcess(driver);
        serverTask = null;
    }

    private void killProcess(ExecutorDriver driver) {
        if (process != null) {
            try {
                // this is only a kind of "last resort" - usual shutdown is performed in killCassandraDaemon() method
                process.destroyForcibly();
            } catch (Exception ignores) {
                // ignore this
            }
            process = null;

            // send health check that process is no longer alive
            healthCheck(driver);
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
                    } catch (Exception ignores) {
                        // ignore this
                    }
                }
            });

            jmxConnect = null;
        }
    }

    @NotNull
    private HealthCheckDetails performHealthCheck() {
        HealthCheckDetails.Builder builder = HealthCheckDetails.newBuilder();

        if (process == null) {
            return builder.setHealthy(false)
                .setMsg("no Cassandra process")
                .build();
        }
        if (jmxConnect == null) {
            return builder.setHealthy(false)
                .setMsg("no JMX connect to Cassandra process")
                .build();
        }

        try {
            NodeInfo info = buildInfo();
            builder.setHealthy(true)
                    .setInfo(info);
            LOGGER.info("Healthcheck succeeded: operationMode:{} joined:{} gossip:{} native:{} rpc:{} uptime:{}s endpoint:{}, dc:{}, rack:{}, hostId:{}, version:{}",
                info.getOperationMode(),
                info.getJoined(),
                info.getGossipRunning(),
                info.getNativeTransportRunning(),
                info.getRpcServerRunning(),
                info.getUptimeMillis() / 1000,
                info.getEndpoint(),
                info.getDataCenter(),
                info.getRack(),
                info.getHostId(),
                info.getVersion());
        } catch (Exception e) {
            LOGGER.warn("Healthcheck failed.", e);
            builder.setHealthy(false)
                   .setMsg(e.toString());
        }
        return builder.build();
    }

    /**
     * Handles the state when the Cassandra server process exited on its own and cleans up our state.
     */
    private void handleProcessNoLongerAlive(ExecutorDriver driver) {
        if (process == null)
            return;
        try {
            int exitCode = process.exitValue();
            LOGGER.error("Cassandra process terminated unexpectedly with exit code {}", exitCode);

            if (serverTask != null) {
                final TaskStatus taskStatus =
                    slaveErrorDetails(serverTask, "process exited with exit code " + exitCode, null, SlaveErrorDetails.ErrorType.PROCESS_EXITED);
                driver.sendStatusUpdate(taskStatus);
            }

            safeShutdown(driver);

            forceCurrentJobAbort();

            // send health check that process is no longer alive
            healthCheck(driver);

        } catch (IllegalThreadStateException e) {
            // ignore
        }
    }

    private NodeInfo buildInfo() throws UnknownHostException {
        Nodetool nodetool = new Nodetool(jmxConnect);

        // C* should be considered healthy, if the information can be collected.
        // All flags can be manually set by any administrator and represent a valid state.

        String operationMode = nodetool.getOperationMode();
        boolean joined = nodetool.isJoined();
        boolean gossipInitialized = nodetool.isGossipInitialized();
        boolean gossipRunning = nodetool.isGossipRunning();
        boolean nativeTransportRunning = nodetool.isNativeTransportRunning();
        boolean rpcServerRunning = nodetool.isRPCServerRunning();

        boolean valid = "NORMAL".equals(operationMode);

        LOGGER.info("Cassandra node status: operationMode={}, joined={}, gossipInitialized={}, gossipRunning={}, nativeTransportRunning={}, rpcServerRunning={}",
                operationMode, joined, gossipInitialized, gossipRunning, nativeTransportRunning, rpcServerRunning);

        NodeInfo.Builder builder = NodeInfo.newBuilder()
                .setOperationMode(operationMode)
                .setJoined(joined)
                .setGossipInitialized(gossipInitialized)
                .setGossipRunning(gossipRunning)
                .setNativeTransportRunning(nativeTransportRunning)
                .setRpcServerRunning(rpcServerRunning)
                .setUptimeMillis(nodetool.getUptimeInMillis())
                .setVersion(nodetool.getVersion())
                .setClusterName(nodetool.getClusterName());

        if (valid) {
            String endpoint = nodetool.getEndpoint();
            builder.setEndpoint(endpoint)
                    .setTokenCount(nodetool.getTokenCount())
                    .setDataCenter(nodetool.getDataCenter(endpoint))
                    .setRack(nodetool.getRack(endpoint))
                    .setHostId(nodetool.getHostID());
        }

        return builder.build();
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

    @NotNull
    private static TaskStatus taskStatus(
        @NotNull final TaskInfo taskInfo,
        @NotNull final TaskState state
    ) {
        return taskStatus(taskInfo, state, nullSlaveStatusDetails());
    }

    @NotNull
    private static TaskStatus taskStatus(
        @NotNull final TaskInfo taskInfo,
        @NotNull final TaskState state,
        @NotNull final SlaveStatusDetails details
    ) {
        return taskStatus(taskInfo.getExecutor().getExecutorId(), taskInfo.getTaskId(), state, details);
    }

    @NotNull
    private static TaskStatus taskStatus(
            @NotNull final ExecutorID executorId,
            @NotNull final TaskID taskId, TaskState state, SlaveStatusDetails details) {
        return TaskStatus.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .setState(state)
            .setSource(TaskStatus.Source.SOURCE_EXECUTOR)
            .setData(ByteString.copyFrom(details.toByteArray()))
            .build();
    }

    private static void serverProcessNotRunning(@NotNull ExecutorDriver driver, @NotNull TaskInfo task) {
        final TaskStatus taskStatus =
            slaveErrorDetails(task, "cassandra server process not running", null, SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING);
        driver.sendStatusUpdate(taskStatus);
    }

    @NotNull
    private static TaskStatus slaveErrorDetails(
        @NotNull TaskInfo task,
        String msg, String details,
        @NotNull SlaveErrorDetails.ErrorType errorType) {
        return slaveErrorDetails(task, msg, details, null, errorType);
    }

    @NotNull
    private static TaskStatus slaveErrorDetails(
        @NotNull TaskInfo task,
        String msg, String details, Integer exitCode,
        @NotNull SlaveErrorDetails.ErrorType errorType) {
        SlaveErrorDetails.Builder slaveErrorDetails = SlaveErrorDetails.newBuilder()
            .setErrorType(errorType);
        if (msg != null)
            slaveErrorDetails.setMsg(msg);
        if (details != null)
            slaveErrorDetails.setDetails(details);
        if (exitCode != null)
            slaveErrorDetails.setProcessExitCode(exitCode);
        SlaveStatusDetails slaveStatusDetails = SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
            .setSlaveErrorDetails(slaveErrorDetails)
            .build();
        return taskStatus(task, TaskState.TASK_ERROR, slaveStatusDetails);
    }

    @NotNull
    private static SlaveStatusDetails nullSlaveStatusDetails() {
        return SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build();
    }
}
