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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.state.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.state.ExecutorMetadata;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.*;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    private static final Joiner JOINER = Joiner.on("','");

    private static final String EXECUTOR_SUFFIX = ".executor";
    private static final String SERVER_SUFFIX = ".server";
    private static final String HEALTHCHECK_SUFFIX = ".healthcheck";
    private static final String REPAIR_SUFFIX = ".repair";
    private static final String REPAIR_STATUS_SUFFIX = ".repair-status";
    private static final String CLEANUP_SUFFIX = ".cleanup";
    private static final String CLEANUP_STATUS_SUFFIX = ".cleanup-status";

    @NotNull
    private String httpServerBaseUrl;

    private final CassandraCluster cluster;

    @NotNull
    private static final Map<String, String> executorEnv = Collections.unmodifiableMap(newHashMap("JAVA_OPTS", "-Xms256m -Xmx256m"));

    public CassandraScheduler(
        @NotNull final CassandraCluster cluster,
        @NotNull final String httpServerBaseUrl
    ) {
        this.httpServerBaseUrl = httpServerBaseUrl;
        this.cluster = cluster;
    }

    @Override
    public void registered(final SchedulerDriver driver, final FrameworkID frameworkId, final MasterInfo masterInfo) {
        LOGGER.debug("registered(driver : {}, frameworkId : {}, masterInfo : {})", driver, protoToString(frameworkId), protoToString(masterInfo));
    }

    @Override
    public void reregistered(final SchedulerDriver driver, final MasterInfo masterInfo) {
        LOGGER.debug("reregistered(driver : {}, masterInfo : {})", driver, protoToString(masterInfo));
    }

    @Override
    public void resourceOffers(final SchedulerDriver driver, final List<Offer> offers) {
        LOGGER.debug("> resourceOffers(driver : {}, offers : {})", driver, protoToString(offers));
        for (final Offer offer : offers) {
            evaluateOffer(driver, offer);
        }
        LOGGER.trace("< resourceOffers(driver : {}, offers : {})", driver, protoToString(offers));
    }

    @Override
    public void offerRescinded(final SchedulerDriver driver, final OfferID offerId) {
        LOGGER.debug("offerRescinded(driver : {}, offerId : {})", driver, protoToString(offerId));
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
        final Marker taskIdMarker = MarkerFactory.getMarker("taskId:" + status.getTaskId().getValue());
        LOGGER.debug(taskIdMarker, "> statusUpdate(driver : {}, status : {})", driver, protoToString(status));
        try {
            ExecutorID executorId = status.getExecutorId();
            TaskID taskId = status.getTaskId();

            SlaveStatusDetails statusDetails;
            if (!status.getData().isEmpty()) {
                statusDetails = SlaveStatusDetails.parseFrom(status.getData());
            } else {
                statusDetails = SlaveStatusDetails.getDefaultInstance();
            }
            LOGGER.info("Status update: {} for task={} executorID={}", status.getState(), taskId.getValue(), executorId.getValue());

            ExecutorMetadata executorMetadata = cluster.metadataForExecutor(executorId);
            if (executorMetadata == null)
                executorMetadata = cluster.metadataForTask(taskId);
            if (executorMetadata == null)
                LOGGER.warn("No metadata for executor {} / task {}", executorId.getValue(), taskId.getValue());

            switch (status.getState()) {
                case TASK_STAGING:
                case TASK_STARTING:
                case TASK_RUNNING:
                    switch (statusDetails.getStatusDetailsType()) {
                        case NULL_DETAILS:
                            break;
                        case ROLLED_OUT_DETAILS:
                            if (executorMetadata != null)
                                executorMetadata.rolledOut();
                            break;
                        case RUN_DETAILS:
                            if (executorMetadata != null)
                                executorMetadata.launched();
                            break;
                        case ERROR_DETAILS:
                            break;
                        default:
                            assert false : "got unexpected status message " + statusDetails.getStatusDetailsType();
                    }
                    break;
                case TASK_FINISHED:
                case TASK_FAILED:
                case TASK_KILLED:
                case TASK_LOST:
                case TASK_ERROR:
                    if (status.getSource() == TaskStatus.Source.SOURCE_SLAVE && status.getReason() == TaskStatus.Reason.REASON_EXECUTOR_TERMINATED) {
                        // this code should really be handled by executorLost, but it can't due to the fact that
                        // executorLost will never be called.

                        cluster.unassociateTaskId(taskId);

                        if (executorMetadata != null) {
                            if (isExecutorTask(taskId)) {
                                if (isExecutorTask(taskId))
                                    executorLost(driver, executorMetadata.getExecutorId(), status.getSlaveId(), status.getState().ordinal());
                            }
                            if (executorMetadata.getServerTaskId().equals(taskId))
                                cluster.serverLost(executorMetadata);
                        }
                    } else {

                        cluster.unassociateTaskId(taskId);

                        if (executorMetadata != null) {
                            if (isExecutorTask(taskId)) {
                                if (isExecutorTask(taskId))
                                    executorLost(driver, executorMetadata.getExecutorId(), status.getSlaveId(), status.getState().ordinal());
                            }
                            if (executorMetadata.getServerTaskId() != null && executorMetadata.getServerTaskId().equals(taskId))
                                cluster.serverLost(executorMetadata);
                        }

                        // TODO trigger node restart on error/failed/lost state

                        switch (statusDetails.getStatusDetailsType()) {
                            case NULL_DETAILS:
                                break;
                            case ROLLED_OUT_DETAILS:
                                if (executorMetadata != null)
                                    executorMetadata.rolledOut();
                                break;
                            case RUN_DETAILS:
                                if (executorMetadata != null)
                                    executorMetadata.launched();
                                break;
                            case ERROR_DETAILS:
                                LOGGER.error(taskIdMarker, protoToString(statusDetails.getSlaveErrorDetails()));
                                break;
                            case HEALTH_CHECK_DETAILS:
                                cluster.updateHealthCheck(executorId, statusDetails.getCassandraNodeHealthCheckDetails());
                                break;
                            case REPAIR_STATUS:
                                cluster.gotRepairStatus(executorId, statusDetails.getKeyspaceJobStatus());
                                break;
                            case CLEANUP_STATUS:
                                cluster.gotCleanupStatus(executorId, statusDetails.getKeyspaceJobStatus());
                                break;
                            default:
                                assert false : "got unexpected status message " + statusDetails.getStatusDetailsType();
                        }
                    }
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error (de)serializing task", e);
        }
        LOGGER.trace(taskIdMarker, "< statusUpdate(driver : {}, status : {})", driver, protoToString(status));
    }

    @Override
    public void frameworkMessage(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
        LOGGER.debug("frameworkMessage(driver : {}, executorId : {}, slaveId : {}, data : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(data));
    }

    @Override
    public void disconnected(final SchedulerDriver driver) {
        LOGGER.debug("disconnected(driver : {})", driver);
        // TODO implement
    }

    @Override
    public void slaveLost(final SchedulerDriver driver, final SlaveID slaveId) {
        LOGGER.debug("slaveLost(driver : {}, slaveId : {})", driver, protoToString(slaveId));
        // TODO implement
    }

    @Override
    public void executorLost(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
        final Marker executorIdMarker = MarkerFactory.getMarker("executorId:" + executorId.getValue());
        // this method will never be called by mesos until MESOS-313 is fixed
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.debug(executorIdMarker, "executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(status));
        cluster.executorLost(executorId);
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    // ---------------------------- Helper methods ---------------------------------------------------------------------

    private static boolean isExecutorTask(TaskID taskId) {
        return taskId.getValue().endsWith(EXECUTOR_SUFFIX);
    }

    private void evaluateOffer(final SchedulerDriver driver, final Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        LOGGER.debug(marker, "> evaluateOffer(driver : {}, offer : {})", driver, protoToString(offer));

        boolean offerUsed = false;

        if (!cluster.hasRequiredNodes()) {
            ExecutorMetadata executorMetadata = cluster.allocateNewExecutor(offer.getHostname());
            if (executorMetadata != null) {
                if (!cluster.hasRequiredSeedNodes()) {
                    cluster.makeSeed(executorMetadata);

                    LOGGER.info(marker, "Allocated executor {} on {}/{} as seed node", executorMetadata.getExecutorId().getValue(), executorMetadata.getHostname(), executorMetadata.getIp());
                }
                else
                    LOGGER.info(marker, "Allocated executor {} on {}/{} as non-seed node", executorMetadata.getExecutorId().getValue(), executorMetadata.getHostname(), executorMetadata.getIp());

                rolloutNode(marker, driver, offer, executorMetadata);
                offerUsed = true;
            }
        }

        for (ExecutorID executorID : offer.getExecutorIdsList()) {
            ExecutorMetadata executorMetadata = cluster.metadataForExecutor(executorID);
            if (executorMetadata == null) {
                LOGGER.warn(marker, "No information about executor {}", executorID.getValue());
                // TODO no information about executor!
                continue;
            }

            if (!executorMetadata.isRunning()) {
                if (cluster.canAddNode() && executorMetadata.shouldTriggerLaunch()) {
                    if (runNode(marker, driver, offer, executorMetadata)) {
                        cluster.nodeRunStateUpdate();
                        offerUsed = true;
                    } else
                        executorMetadata.notLaunched();
                } else if (executorMetadata.isLaunched()) {
                    // TODO this state is reached after the C* daemon has been launched but before we recognize it as running.

                    executorMetadata.setRunning();

                    cluster.nodeRunStateUpdate();
                }
            }

            if (executorMetadata.isRunning()) {
                if (cluster.shouldRunHealthCheck(executorID)) {
                    submitHealthCheck(marker, driver, offer, executorMetadata);
                    offerUsed = true;
                } else if (cluster.shouldGetRepairStatusOnExecutor(executorID)) {
                    submitRepairStatus(marker, driver, offer, executorMetadata, KeyspaceJobType.REPAIR, REPAIR_STATUS_SUFFIX);
                    offerUsed = true;
                } else if (cluster.shouldGetCleanupStatusOnExecutor(executorID)) {
                    submitRepairStatus(marker, driver, offer, executorMetadata, KeyspaceJobType.CLEANUP, CLEANUP_STATUS_SUFFIX);
                    offerUsed = true;
                } else if (cluster.shouldStartRepairOnExecutor(executorID)) {
                    submitStartKeyspaceJob(marker, driver, offer, executorMetadata, KeyspaceJobType.REPAIR, REPAIR_SUFFIX);
                    offerUsed = true;
                } else if (cluster.shouldStartCleanupOnExecutor(executorID)) {
                    submitStartKeyspaceJob(marker, driver, offer, executorMetadata, KeyspaceJobType.CLEANUP, CLEANUP_SUFFIX);
                    offerUsed = true;
                }
            }
        }

        if (!offerUsed)
            driver.declineOffer(offer.getId());
    }

    private void submitRepairStatus(Marker marker, SchedulerDriver driver, Offer offer, ExecutorMetadata executorMetadata,
                                    KeyspaceJobType keyspaceJobType, String suffix) {
        TaskID taskId = cluster.createTaskId(executorMetadata, suffix);
        TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_KEYSPACE_JOB_STATUS)
                .setCassandraNodeKeyspaceJobStatusTask(CassandraNodeKeyspaceJobStatusTask.newBuilder().setType(keyspaceJobType))
                .build();
        TaskInfo task = TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();
        LOGGER.debug(marker, "Launching CASSANDRA_NODE_REPAIR_STATUS task", protoToString(task));
        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
    }

    private void submitStartKeyspaceJob(Marker marker, SchedulerDriver driver, Offer offer, ExecutorMetadata executorMetadata,
                                        KeyspaceJobType keyspaceJobType, String suffix) {
        TaskID taskId = cluster.createTaskId(executorMetadata, suffix);
        TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_KEYSPACE_JOB)
                .setCassandraNodeKeyspaceJobTask(CassandraNodeKeyspaceJobTask.newBuilder()
                        .setType(keyspaceJobType)
                        .setJmx(jmxConnect(executorMetadata)))
                .build();
        TaskInfo task = TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();
        LOGGER.debug(marker, "Launching CASSANDRA_NODE_REPAIR task", protoToString(task));
        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));

    }

    private void submitHealthCheck(Marker marker, SchedulerDriver driver, Offer offer, ExecutorMetadata executorMetadata) {
        TaskID taskId = cluster.createTaskId(executorMetadata, HEALTHCHECK_SUFFIX);
        TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_HEALTH_CHECK)
                .setCassandraNodeHealthCheckTask(
                        CassandraNodeHealthCheckTask.newBuilder()
                                .setJmx(jmxConnect(executorMetadata))
                                .build()
                )
                .build();
        TaskInfo task = TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();
        LOGGER.debug(marker, "Launching CASSANDRA_NODE_HEALTH_CHECK task", protoToString(task));
        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
    }

    private void rolloutNode(Marker marker, SchedulerDriver driver, Offer offer, ExecutorMetadata executorMetadata) {
        String osName = Env.option("OS_NAME").or(Env.osFromSystemProperty());
        String javaExec = "macosx".equals(osName)
                ? "$(pwd)/jre*/Contents/Home/bin/java"
                : "$(pwd)/jre*/bin/java";

        ExecutorInfo info = executorInfo(
                executorMetadata.getExecutorId(),
                executorMetadata.getExecutorId().getValue(),
                cluster.getName(),
                commandInfo(
                        javaExec + " $JAVA_OPTS -classpath cassandra-executor.jar io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor",
                        environmentFromMap(executorEnv),
                        commandUri(getUrlForResource("/jre-" + osName + ".tar.gz"), true),
                        commandUri(getUrlForResource("/apache-cassandra-" + cluster.getCassandraVersion() + "-bin.tar.gz"), true),
                        commandUri(getUrlForResource("/cassandra-executor.jar"))
                ),
                cpu(0.1),
                mem(256),
                disk(16)
        );

        TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_ROLLOUT)
                .build();
        TaskID taskId = taskId(executorMetadata.getExecutorId().getValue());
        TaskInfo task = TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(newArrayList(
                        cpu(0.1),
                        mem(16),
                        disk(16)
                ))
                .setExecutor(info)
                .build();
        LOGGER.debug(marker, "Launching executor = {} with task = {}", protoToString(info), protoToString(task));

        executorMetadata.setExecutorInfo(info, taskId);
        cluster.associateTaskId(executorMetadata, taskId);

        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
    }

    private boolean runNode(Marker marker, SchedulerDriver driver, Offer offer, ExecutorMetadata executorMetadata) {
        final List<String> errors = cluster.checkResources(offer, executorMetadata);
        if (!errors.isEmpty()) {
            LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
            return false;
        }

        TaskConfig taskConfig = TaskConfig.newBuilder()
                .addVariables(TaskConfig.Entry.newBuilder().setName("cluster_name").setStringValue(cluster.getName()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("broadcast_address").setStringValue(executorMetadata.getIp()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("rpc_address").setStringValue(executorMetadata.getIp()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("listen_address").setStringValue(executorMetadata.getIp()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("storage_port").setLongValue(cluster.getStoragePort()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("ssl_storage_port").setLongValue(cluster.getSslStoragePort()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("native_transport_port").setLongValue(cluster.getNativePort()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("rpc_port").setLongValue(cluster.getRpcPort()))
                .addVariables(TaskConfig.Entry.newBuilder().setName("seeds").setStringValue(Joiner.on(',').join(newArrayList(cluster.seedsIpList()))))
                .build();
        TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_RUN)
                .setCassandraNodeRunTask(
                        CassandraNodeRunTask.newBuilder()
                                .setVersion(cluster.getCassandraVersion())
                                .addAllCommand(newArrayList("apache-cassandra-" + cluster.getCassandraVersion() + "/bin/cassandra", "-p", "cassandra.pid"))
                                .setTaskEnv(taskEnv(
                                        // see conf/cassandra-env.sh in the cassandra distribution for details
                                        // about these variables.
                                        tuple2("JMX_PORT", String.valueOf(executorMetadata.getJmxPort())),
                                        tuple2("MAX_HEAP_SIZE", cluster.getMemMb() + "m"),
                                        // The example HEAP_NEWSIZE assumes a modern 8-core+ machine for decent pause
                                        // times. If in doubt, and if you do not particularly want to tweak, go with
                                        // 100 MB per physical CPU core.
                                        tuple2("HEAP_NEWSIZE", (int) (cluster.getCpuCores() * 100) + "m")
                                ))
                                .setTaskConfig(taskConfig)
                )
                .build();
        TaskID taskId = taskId(executorMetadata.getExecutorId().getValue() + SERVER_SUFFIX);
        TaskInfo task = TaskInfo.newBuilder()
                .setName(taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(cluster.resourcesForExecutor(executorMetadata))
                .setExecutor(executorMetadata.getExecutorInfo())
                .build();

        LOGGER.debug(marker, "Launching CASSANDRA_NODE_LAUNCH task = {}", protoToString(task));

        cluster.associateTaskId(executorMetadata, taskId);
        executorMetadata.setServerTaskId(taskId);

        driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));

        return true;
    }

    private static JmxConnect jmxConnect(ExecutorMetadata executorMetadata) {
        return JmxConnect.newBuilder()
                .setJmxPort(executorMetadata.getJmxPort())
                        // TODO add jmxSsl, jmxUsername, jmxPassword
                .setIp(executorMetadata.getIp())
                .build();
    }

    @NotNull
    @SafeVarargs
    private static TaskEnv taskEnv(@NotNull final Tuple2<String, String>... tuples) {
        return TaskEnv.newBuilder()
                .addAllVariables(from(newArrayList(tuples)).transform(tupleToTaskEnvEntry))
                .build();
    }

    private static final Pattern URL_FOR_RESOURCE_PATTERN = Pattern.compile("(?<!:)/+");

    @NotNull
    private String getUrlForResource(@NotNull final String resourceName) {
        return URL_FOR_RESOURCE_PATTERN.matcher((httpServerBaseUrl + '/' + resourceName)).replaceAll("/");
    }

    @NotNull
    private static <K, V> Map<K, V> newHashMap(@NotNull final K key, @NotNull final V value) {
        final HashMap<K, V> map = Maps.newHashMap();
        map.put(key, value);
        return map;
    }

    private static final Function<Tuple2<String, String>, TaskEnv.Entry> tupleToTaskEnvEntry = new Function<Tuple2<String, String>, TaskEnv.Entry>() {
        @Override
        public TaskEnv.Entry apply(final Tuple2<String, String> input) {
            return TaskEnv.Entry.newBuilder().setName(input._1).setValue(input._2).build();
        }
    };
}
