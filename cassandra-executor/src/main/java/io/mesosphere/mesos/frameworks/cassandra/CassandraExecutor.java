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

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.jmx.JmxConnect;
import io.mesosphere.mesos.frameworks.cassandra.jmx.NodeRepairJob;
import io.mesosphere.mesos.frameworks.cassandra.jmx.Nodetool;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.*;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

    private volatile Process process;
    private volatile TaskID serverTaskID;
    private volatile Boolean lastKnownStatus;
    private final AtomicReference<NodeRepairJob> repair = new AtomicReference<>();

    @Override
    public void registered(final ExecutorDriver driver, final ExecutorInfo executorInfo, final FrameworkInfo frameworkInfo, final SlaveInfo slaveInfo) {
        LOGGER.debug("registered(driver : {}, executorInfo : {}, frameworkInfo : {}, slaveInfo : {})", driver, protoToString(executorInfo), protoToString(frameworkInfo), protoToString(slaveInfo));
    }

    @Override
    public void reregistered(final ExecutorDriver driver, final SlaveInfo slaveInfo) {
        LOGGER.debug("reregistered(driver : {}, slaveInfo : {})", driver, protoToString(slaveInfo));
    }

    @Override
    public void disconnected(final ExecutorDriver driver) {
        LOGGER.debug("disconnected(driver : {})", driver);
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        final Marker taskIdMarker = MarkerFactory.getMarker(task.getTaskId().getValue());
        LOGGER.debug(taskIdMarker, "> launchTask(driver : {}, task : {})", driver, protoToString(task));
        try {
            driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_STARTING));
            final ByteString data = task.getData();
            final TaskDetails taskDetails = TaskDetails.parseFrom(data);
            LOGGER.debug(taskIdMarker, "received taskDetails: {}", protoToString(taskDetails));
            switch (taskDetails.getTaskType()) {
                case SLAVE_METADATA:
                    final SlaveMetadata slaveMetadata = collectSlaveMetadata();
                    final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.SLAVE_METADATA)
                        .setSlaveMetadata(slaveMetadata)
                        .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_RUNNING, details));
                    break;
                case CASSANDRA_NODE_RUN:
                    final Process cassandraProcess = launchCassandraNodeTask(taskIdMarker, taskDetails.getCassandraNodeRunTask());
                    process = cassandraProcess;
                    serverTaskID = task.getTaskId();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_STARTING));
                    break;
                case CASSANDRA_NODE_SHUTDOWN:
                    process.destroy();
                    process = null;
                    driver.sendStatusUpdate(taskStatus(task.getExecutor().getExecutorId(),
                            serverTaskID,
                            TaskState.TASK_FINISHED,
                            nullSlaveStatusDetails()));
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED));
                    break;
                case CASSANDRA_NODE_HEALTH_CHECK:
                    final CassandraNodeHealthCheckTask healthCheckTask = taskDetails.getCassandraNodeHealthCheckTask();
                    LOGGER.info(taskIdMarker, "Received healthCheckTask: {}", protoToString(healthCheckTask));
                    final CassandraNodeHealthCheckDetails healthCheck = performHealthCheck(healthCheckTask);
                    final SlaveStatusDetails healthCheckDetails = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                        .setCassandraNodeHealthCheckDetails(healthCheck)
                        .build();
                    if (lastKnownStatus == null || healthCheck.getHealthy() != lastKnownStatus) {
                        lastKnownStatus = healthCheck.getHealthy();
                        driver.sendStatusUpdate(taskStatus(task.getExecutor().getExecutorId(),
                                serverTaskID,
                                healthCheck.getHealthy() ? TaskState.TASK_RUNNING : TaskState.TASK_ERROR,
                                nullSlaveStatusDetails()));
                    }
                    driver.sendStatusUpdate(taskStatus(task, healthCheck.getHealthy() ? TaskState.TASK_FINISHED : TaskState.TASK_ERROR, healthCheckDetails));
                    break;
                case CASSANDRA_NODE_REPAIR:
                    CassandraNodeRepairTask repairTask = taskDetails.getCassandraNodeRepairTask();
                    NodeRepairJob currentRepair = repair.get();
                    if (currentRepair == null || currentRepair.isFinished()) {
                        repair.set(currentRepair = new NodeRepairJob());
                        currentRepair.start(new JmxConnect(repairTask.getJmx()));
                        currentRepair.repairNextKeyspace();
                    }
                case CASSANDRA_NODE_REPAIR_STATUS:
                    currentRepair = repair.get();
                    CassandraNodeRepairStatus.Builder repairStatus = CassandraNodeRepairStatus.newBuilder()
                            .setRunning(currentRepair != null && !currentRepair.isFinished());
                    if (currentRepair != null) {
                        repairStatus.addAllRemainingKeyspaces(currentRepair.getRemainingKeyspaces())
                                .addAllRepairedKeyspaces(currentRepair.getKeyspaceStatus().values())
                            .setStarted(currentRepair.getStartTimestamp())
                            .setFinished(currentRepair.getFinishedTimestamp());
                    }
                    SlaveStatusDetails repairDetails = SlaveStatusDetails.newBuilder()
                            .setRepairStatus(repairStatus.build())
                            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.REPAIR_STATUS)
                            .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED, repairDetails));
                    break;
                case CASSANDRA_NODE_CLEANUP:
                    // TODO implement
                case CASSANDRA_NODE_CLEANUP_STATUS:
                    // TODO implement
                    SlaveStatusDetails cleanupDetails = SlaveStatusDetails.newBuilder()
                            .setCleanupStatus(CassandraNodeCleanupStatus.getDefaultInstance())
                            .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED, cleanupDetails));
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task data to type: " + TaskDetails.class.getName();
            LOGGER.error(taskIdMarker, msg, e);
            final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                    .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
                    .setSlaveErrorDetails(
                            SlaveErrorDetails.newBuilder()
                                    .setMsg(msg)
                    )
                .build();
            final TaskStatus taskStatus = taskStatus(task, TaskState.TASK_ERROR, details);
            driver.sendStatusUpdate(taskStatus);
        } catch (VirtualMachineError e) {
            throw e;
        } catch (Throwable e) {
            final String msg = "Error starting task due to exception.";
            LOGGER.error(taskIdMarker, msg, e);
            final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                    .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
                    .setSlaveErrorDetails(
                            SlaveErrorDetails.newBuilder()
                                    .setMsg(msg)
                                    .setDetails(e.getMessage())
                    )
                .build();
            final TaskStatus taskStatus = taskStatus(task, TaskState.TASK_ERROR, details);
            driver.sendStatusUpdate(taskStatus);
        }
        LOGGER.debug(taskIdMarker, "< launchTask(driver : {}, task : {})", driver, protoToString(task));
    }

    @Override
    public void killTask(final ExecutorDriver driver, final TaskID taskId) {
        LOGGER.debug("killTask(driver : {}, taskId : {})", driver, protoToString(taskId));
    }

    @Override
    public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
        LOGGER.debug("frameworkMessage(driver : {}, data : {})", driver, data);
    }

    @Override
    public void shutdown(final ExecutorDriver driver) {
        LOGGER.debug("shutdown(driver : {})", driver);
    }

    @Override
    public void error(final ExecutorDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    private static SlaveMetadata collectSlaveMetadata() throws UnknownHostException {
        return SlaveMetadata.newBuilder()
            .setIp(getHostAddress())
            .build();
    }

    private static String getHostAddress() throws UnknownHostException {
        // TODO: what to do on multihomed hosts?
        return InetAddress.getLocalHost().getHostAddress(); //TODO(BenWhitehead): This resolution may have to be more sophisticated
    }

    @NotNull
    private static Process launchCassandraNodeTask(@NotNull final Marker taskIdMarker, @NotNull final CassandraNodeRunTask cassandraNodeTask) throws IOException {
        LOGGER.info(taskIdMarker, "Building cassandra.yaml");

        File cassandraYaml = new File("apache-cassandra-" + cassandraNodeTask.getVersion() + "/conf/cassandra.yaml");

        Yaml yaml = new Yaml();
        Map<String, Object> yamlMap;
        LOGGER.info(taskIdMarker, "Reading cassandra.yaml");
        try (BufferedReader br = new BufferedReader(new FileReader(cassandraYaml))) {
            yamlMap = (Map<String, Object>) yaml.load(br);
        }
        LOGGER.info(taskIdMarker, "Modifying cassandra.yaml");
        for (TaskConfig.Entry entry : cassandraNodeTask.getTaskConfig().getVariablesList()) {
            switch (entry.getName()) {
                case "seeds":
                    List<Map<String, Object>> seedProviderList = (List<Map<String, Object>>) yamlMap.get("seed_provider");
                    Map<String, Object> seedProviderMap = seedProviderList.get(0);
                    List<Map<String, Object>> parameters = (List<Map<String, Object>>) seedProviderMap.get("parameters");
                    Map<String, Object> parametersMap = parameters.get(0);
                    parametersMap.put("seeds", entry.getStringValue());
                    break;
                default:
                    if (entry.hasStringValue())
                        yamlMap.put(entry.getName(), entry.getStringValue());
                    else if (entry.hasLongValue())
                        yamlMap.put(entry.getName(), entry.getLongValue());
            }
        }
        if (LOGGER.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            yaml.dump(yamlMap, sw);
            LOGGER.debug("cassandra.yaml result: {}", sw);
        }
        LOGGER.info(taskIdMarker, "Writing cassandra.yaml");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(cassandraYaml))) {
            yaml.dump(yamlMap, bw);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder(cassandraNodeTask.getCommandList())
                .directory(new File(System.getProperty("user.dir")))
                .redirectOutput(new File("cassandra-stdout.log"))
                .redirectError(new File("cassandra-stderr.log"));
        for (final TaskEnv.Entry entry : cassandraNodeTask.getTaskEnv().getVariablesList()) {
            processBuilder.environment().put(entry.getName(), entry.getValue());
        }
        processBuilder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        LOGGER.debug("Starting Process: {}", processBuilderToString(processBuilder));
        return processBuilder.start();
    }

    @NotNull
    private static CassandraNodeHealthCheckDetails performHealthCheck(@NotNull final CassandraNodeHealthCheckTask healthCheckTask) {
        CassandraNodeHealthCheckDetails.Builder builder = CassandraNodeHealthCheckDetails.newBuilder();
        CassandraTaskProtos.JmxConnect jmxInfo = healthCheckTask.getJmx();
        try (JmxConnect jmx = new JmxConnect(jmxInfo)) {

            // TODO Robert : we should add some "timeout" that allows C* to start and join
            // I.e. track status/operation-mode changes

            CassandraNodeInfo info = buildInfo(jmx);
            builder.setInfo(info)
                   .setHealthy(true);
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

    private static CassandraNodeInfo buildInfo(JmxConnect jmx) throws UnknownHostException {
        Nodetool nodetool = new Nodetool(jmx);

        String endpoint = nodetool.getEndpoint();

        // TODO HC response might be a bit over-engineered

        // C* should be considered healthy, if the information can be collected.
        // All flags can be manually set by any administrator and represent a valid state.
        return CassandraNodeInfo.newBuilder()
                .setOperationMode(nodetool.getOperationMode())
                .setJoined(nodetool.isJoined())
                .setGossipInitialized(nodetool.isGossipInitialized())
                .setGossipRunning(nodetool.isGossipRunning())
                .setNativeTransportRunning(nodetool.isNativeTransportRunning())
                .setRpcServerRunning(nodetool.isRPCServerRunning())
                .setUptimeMillis(nodetool.getUptimeInMillis())
                .setVersion(nodetool.getVersion())
                .setHostId(nodetool.getHostID())
                .setEndpoint(endpoint)
                .setTokenCount(nodetool.getTokenCount())
                .setDataCenter(nodetool.getDataCenter(endpoint))
                .setRack(nodetool.getRack(endpoint))
                .setClusterName(nodetool.getClusterName())
                .build();
    }

    public static void main(final String[] args) {
        final MesosExecutorDriver driver = new MesosExecutorDriver(new CassandraExecutor());
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
        @NotNull final ExecutorID executorID,
        @NotNull final TaskID taskID,
        @NotNull final TaskState state,
        @NotNull final SlaveStatusDetails details
    ) {
        return TaskStatus.newBuilder()
                .setExecutorId(executorID)
                .setTaskId(taskID)
                .setState(state)
                .setSource(TaskStatus.Source.SOURCE_EXECUTOR)
                .setData(ByteString.copyFrom(details.toByteArray()))
                .build();
    }

    @NotNull
    private static SlaveStatusDetails nullSlaveStatusDetails() {
        return SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build();
    }

    @NotNull
    private static String processBuilderToString(@NotNull final ProcessBuilder builder) {
        final StringBuilder sb = new StringBuilder("ProcessBuilder{\n");
        sb.append("directory() = ").append(builder.directory());
        sb.append(",\n");
        sb.append("command() = ").append(Joiner.on(" ").join(builder.command()));
        sb.append(",\n");
        sb.append("environment() = ").append(Joiner.on("\n").withKeyValueSeparator("->").join(builder.environment()));
        sb.append("\n}");
        return sb.toString();
    }
}
