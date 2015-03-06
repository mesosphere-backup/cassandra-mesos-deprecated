/**
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
package io.mesosphere.mesos.frameworks.cassandra;

import ch.qos.logback.classic.LoggerContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.jmx.*;
import io.mesosphere.mesos.frameworks.cassandra.jmx.JmxConnect;
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
import java.util.concurrent.atomic.AtomicReference;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

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
    private volatile boolean serverTaskStatusOutstanding;

    private ExecutorInfo executorInfo;
    private TaskInfo serverTask;

    private final AtomicReference<AbstractNodeJob> currentJob = new AtomicReference<>();

    private final ExecutorService executorService;

    public CassandraExecutor(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;

        this.executorService = new ThreadPoolExecutor(0, 2, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
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

            processAlive(driver);

            switch (taskDetails.getTaskType()) {
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
                    safeShutdown();
                    serverTask = task;
                    serverTaskStatusOutstanding = true;
                    process = objectFactory.launchCassandraNodeTask(taskIdMarker, taskDetails.getCassandraServerRunTask());
                    jmxConnect = objectFactory.newJmxConnect(taskDetails.getCassandraServerRunTask().getJmx());
                    break;
                case CASSANDRA_SERVER_SHUTDOWN:
                    safeShutdown();
                    driver.sendStatusUpdate(taskStatus(task.getExecutor().getExecutorId(),
                            serverTask.getTaskId(),
                            TaskState.TASK_FINISHED,
                            nullSlaveStatusDetails()));
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED));
                    break;
                case HEALTH_CHECK:
                    final HealthCheckTask healthCheckTask = taskDetails.getHealthCheckTask();
                    LOGGER.info(taskIdMarker, "Received healthCheckTask: {}", protoToString(healthCheckTask));
                    final HealthCheckDetails healthCheck = performHealthCheck(driver);
                    final SlaveStatusDetails healthCheckDetails = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                        .setHealthCheckDetails(healthCheck)
                        .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED, healthCheckDetails));
                    break;
                case NODE_JOB:
                    startJob(driver, task, taskDetails);
                    break;
                case NODE_JOB_STATUS:
                    jobStatus(driver, task);
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task data to type: " + TaskDetails.class.getName();
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                slaveErrorDetails(task, msg, null, null);
            driver.sendStatusUpdate(taskStatus);
        } catch (Exception e) {
            final String msg = "Error starting task due to exception.";
            LOGGER.error(taskIdMarker, msg, e);
            final TaskStatus taskStatus =
                slaveErrorDetails(task, msg, e.getMessage() != null ? e.getMessage() : "-", null);
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
    }

    @Override
    public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frameworkMessage(driver : {}, data : {})", driver, data);
        }

        try {
            TaskDetails taskDetails = TaskDetails.parseFrom(data);

            processAlive(driver);

            switch (taskDetails.getTaskType()) {
                case HEALTH_CHECK:
                    final HealthCheckTask healthCheckTask = taskDetails.getHealthCheckTask();
                    LOGGER.info("Received healthCheckTask: {}", protoToString(healthCheckTask));
                    final HealthCheckDetails healthCheck = performHealthCheck(driver);
                    final SlaveStatusDetails healthCheckDetails = SlaveStatusDetails.newBuilder()
                            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                            .setHealthCheckDetails(healthCheck)
                            .build();
                    driver.sendFrameworkMessage(healthCheckDetails.toByteArray());
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

    @NotNull
    private static ExecutorMetadata collectSlaveMetadata(@NotNull final ExecutorMetadataTask executorMetadata) {
        return ExecutorMetadata.newBuilder()
            .setExecutorId(executorMetadata.getExecutorId())
            .setIp(executorMetadata.getIp())
            .build();
    }

    private void safeShutdown() {
        if (jmxConnect != null) {
            try {
                jmxConnect.close();
            } catch (Exception ignores) {
                // ignore this
            }
            jmxConnect = null;
        }
        if (process != null) {
            try {
                process.destroy();
            } catch (Exception ignores) {
                // ignore this
            }
            process = null;
        }
        serverTask = null;
    }

    @NotNull
    private HealthCheckDetails performHealthCheck(ExecutorDriver driver) {
        HealthCheckDetails.Builder builder = HealthCheckDetails.newBuilder();

        if (serverTaskStatusOutstanding) {
            driver.sendStatusUpdate(taskStatus(serverTask, process != null ? TaskState.TASK_RUNNING : TaskState.TASK_ERROR));
            serverTaskStatusOutstanding = false;
        }

        if (process == null) {
            return builder.setHealthy(false)
                    .setMsg("no Cassandra process")
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

    private void processAlive(ExecutorDriver driver) {
        if (process == null)
            return;
        try {
            int exitCode = process.exitValue();
            LOGGER.error("Cassandra process terminated unexpectedly with exit code {}", exitCode);

            final TaskStatus taskStatus =
                slaveErrorDetails(serverTask, "process exited with exit code " + exitCode, null, SlaveErrorDetails.ErrorType.PROCESS_EXITED);
            driver.sendStatusUpdate(taskStatus);

            safeShutdown();

            forceCurrentJobAbort();

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
        String msg, String details, SlaveErrorDetails.ErrorType errorType) {
        SlaveErrorDetails.Builder slaveErrorDetails = SlaveErrorDetails.newBuilder();
        if (msg != null)
            slaveErrorDetails.setMsg(msg);
        if (details != null)
            slaveErrorDetails.setDetails(details);
        if (errorType != null)
            slaveErrorDetails.setErrorType(errorType);
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
