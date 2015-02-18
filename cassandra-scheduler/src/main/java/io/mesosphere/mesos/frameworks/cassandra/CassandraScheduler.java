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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.bindown.JreLoader;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import io.mesosphere.mesos.util.Clock;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.*;
import static io.mesosphere.mesos.util.Functions.headOption;
import static io.mesosphere.mesos.util.Functions.unmodifiableHashMap;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    private static final Joiner JOINER = Joiner.on("','");

    // see: http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
    private static final Map<String, Long> defaultCassandraPortMappings = unmodifiableHashMap(
        tuple2("storage_port", 7000L),
        tuple2("ssl_storage_port", 7001L),
        tuple2("jmx", 7199L),
        tuple2("native_transport_port", 9042L),
        tuple2("rpc_port", 9160L)
    );
    private static final Pattern URL_FOR_RESOURCE_PATTERN = Pattern.compile("(?<!:)/+");

    private static final String EXECUTOR_SUFFIX = ".executor";
    private static final String HEALTHCHECK_SUFFIX = ".healthcheck";
    private static final String REPAIR_SUFFIX = ".repair";
    private static final String REPAIR_STATUS_SUFFIX = ".repair-status";

    @NotNull
    private final AtomicInteger execCounter;
    @NotNull
    private final Clock clock;
    @NotNull
    private final String frameworkName;
    @NotNull
    private String httpServerBaseUrl;
    private final int numberOfNodes;
    private final double cpuCores;
    private final long memMb;
    private final long diskMb;
    private final Duration healthCheckInterval;
    private final String cassandraVersion;

    @NotNull
    private final List<SuperTask> superTasksList;
    private final Lock superTasksLock = new ReentrantLock();

    @NotNull
    private final Map<String, String> executorEnv;

    public CassandraScheduler(
        @NotNull final Clock clock,
        @NotNull final String frameworkName,
        @NotNull final String httpServerBaseUrl,
        final int numberOfNodes,
        final double cpuCores,
        final long memMb,
        final long diskMb,
        final long healthCheckIntervalSeconds,
        final String cassandraVersion
    ) {
        this.clock = clock;
        this.frameworkName = frameworkName;
        this.httpServerBaseUrl = httpServerBaseUrl;
        this.numberOfNodes = numberOfNodes;
        this.cpuCores = cpuCores;
        this.memMb = memMb;
        this.diskMb = diskMb;
        healthCheckInterval = Duration.standardSeconds(healthCheckIntervalSeconds);
        this.cassandraVersion = cassandraVersion;

        execCounter = new AtomicInteger(0);
        superTasksList = Lists.newArrayList();
        executorEnv = Collections.unmodifiableMap(newHashMap("JAVA_OPTS", "-Xms256m -Xmx256m"));

        CassandraCluster.instance().setName(frameworkName);
        CassandraCluster.instance().setStoragePort(defaultCassandraPortMappings.get("storage_port").intValue());
        CassandraCluster.instance().setSslStoragePort(defaultCassandraPortMappings.get("ssl_storage_port").intValue());
        CassandraCluster.instance().setNativePort(defaultCassandraPortMappings.get("native_transport_port").intValue());
        CassandraCluster.instance().setRpcPort(defaultCassandraPortMappings.get("rpc_port").intValue());
    }

    private List<SuperTask> superTasks() {
        superTasksLock.lock();
        try {
            return Lists.newArrayList(superTasksList);
        } finally {
            superTasksLock.unlock();
        }
    }

    private void addSuperTask(SuperTask superTask) {
        superTasksLock.lock();
        try {
            superTasksList.add(superTask);
        } finally {
            superTasksLock.unlock();
        }
    }

    private void removeSuperTask(ExecutorID executorId) {
        superTasksLock.lock();
        try {
            for (Iterator<SuperTask> i = superTasksList.iterator(); i.hasNext();) {
                SuperTask superTask = i.next();
                if (executorId.equals(superTask.getExecutorInfo().getExecutorId()))
                    i.remove();
            }
        } finally {
            superTasksLock.unlock();
        }
    }

    private void removeSuperTask(TaskID taskId) {
        superTasksLock.lock();
        try {
            for (Iterator<SuperTask> i = superTasksList.iterator(); i.hasNext();) {
                SuperTask superTask = i.next();
                if (taskId.equals(superTask.getTaskInfo().getTaskId()))
                    i.remove();
            }
        } finally {
            superTasksLock.unlock();
        }
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
            final ExecutorID executorId = status.getExecutorId();
            final TaskID taskId = status.getTaskId();
            final SlaveStatusDetails statusDetails;
            if (!status.getData().isEmpty()) {
                statusDetails = SlaveStatusDetails.parseFrom(status.getData());
            } else {
                statusDetails = SlaveStatusDetails.getDefaultInstance();
            }
            LOGGER.info("Status update: {} for task={} executorID={}", status.getState(), taskId.getValue(), executorId.getValue());
            switch (status.getState()) {
                case TASK_STAGING:
                case TASK_STARTING:
                case TASK_RUNNING:
                    switch (statusDetails.getStatusDetailsType()) {
                        case NULL_DETAILS:
                            break;
                        case SLAVE_METADATA:
                            CassandraCluster.instance().executorMetadata(executorId).setSlaveMetadata(statusDetails.getSlaveMetadata());
                            break;
                        case ERROR_DETAILS:
                            break;
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

                        // there is the possibility that the executorId we get in the task status is empty,
                        // so here we use the taskId to lookup the executorId based on the tasks we're tracking
                        // to try and have a more accurate value.
                        final ExecutorID executorIdForTask = headOption(
                            from(superTasks()).filter(SuperTask.taskIdEq(taskId)).transform(SuperTask.toExecutorId())
                        ).or(executorId);
                        executorLost(driver, executorIdForTask, status.getSlaveId(), status.getState().ordinal());
                    } else {

                        removeSuperTask(taskId);
                        if (isMainTask(taskId))
                            CassandraCluster.instance().executorMetadata(executorId).clear();

                        // TODO trigger node restart on error/failed/lost state

                        switch (statusDetails.getStatusDetailsType()) {
                            case NULL_DETAILS:
                                break;
                            case SLAVE_METADATA:
                                break;
                            case ERROR_DETAILS:
                                LOGGER.error(taskIdMarker, protoToString(statusDetails.getSlaveErrorDetails()));
                                break;
                            case HEALTH_CHECK_DETAILS:
                                CassandraCluster.instance().executorMetadata(executorId).updateHealthCheck(clock.now(), statusDetails.getCassandraNodeHealthCheckDetails());
                                break;
                            case REPAIR_STATUS:
                                CassandraCluster.instance().gotRepairStatus(executorId, statusDetails.getRepairStatus());
                                break;
                            case CLEANUP_STATUS:
                                // TODO implement
                                break;
                        }
                    }
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task stats data to type: " + SlaveMetadata.class.getName();
            LOGGER.error(msg, e);
        }
        LOGGER.trace(taskIdMarker, "< statusUpdate(driver : {}, status : {})", driver, protoToString(status));
    }

    private static boolean isMainTask(TaskID taskId) {
        return taskId.getValue().endsWith(EXECUTOR_SUFFIX);
    }

    @Override
    public void frameworkMessage(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
        LOGGER.debug("frameworkMessage(driver : {}, executorId : {}, slaveId : {}, data : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(data));
    }

    @Override
    public void disconnected(final SchedulerDriver driver) {
        LOGGER.debug("disconnected(driver : {})", driver);
    }

    @Override
    public void slaveLost(final SchedulerDriver driver, final SlaveID slaveId) {
        LOGGER.debug("slaveLost(driver : {}, slaveId : {})", driver, protoToString(slaveId));
    }

    @Override
    public void executorLost(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
        final Marker executorIdMarker = MarkerFactory.getMarker("executorId:" + executorId.getValue());
        // this method will never be called by mesos until MESOS-313 is fixed
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.debug(executorIdMarker, "executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(status));
        removeSuperTask(executorId);
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    // ---------------------------- Helper methods ---------------------------------------------------------------------

    /**
     * @return boolean representing if the the offer was used
     */
    private boolean evaluateOffer(final SchedulerDriver driver, final Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        LOGGER.debug(marker, "> evaluateOffer(driver : {}, offer : {})", driver, protoToString(offer));
        boolean offerUsed = false;
        final ImmutableListMultimap<ExecutorID, SuperTask> tasksByExecutor = from(superTasks()).index(SuperTask.toExecutorId());
        if (maybeLaunchExecutor(driver, offer, marker, tasksByExecutor)) {
            LOGGER.trace(marker, "< evaluateOffer(driver : {}, offer : {})", driver, protoToString(offer));
            return true;
        }

        for (final ExecutorID executorID : offer.getExecutorIdsList()) {
            final ImmutableList<SuperTask> tasks = tasksByExecutor.get(executorID);
            if (!tasks.isEmpty()) {
                final SuperTask head = tasks.get(0);
                final ExecutorInfo info = head.getExecutorInfo();
                final boolean nodeNotAlreadyRunning = from(tasks).filter(SuperTask.taskDetailsTypeEq(TaskDetails.TaskType.CASSANDRA_NODE_RUN)).isEmpty();
                if (nodeNotAlreadyRunning) {
                    final List<String> errors = hasResources(offer, cpuCores, memMb, diskMb, defaultCassandraPortMappings);
                    if (!errors.isEmpty()) {
                        LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                        continue;
                    }

                    ExecutorMetadata executorMetadata = CassandraCluster.instance().executorMetadata(executorID);
                    executorMetadata.updateHostname(offer.getHostname());
                    TaskConfig taskConfig = TaskConfig.newBuilder()
                            .addVariables(TaskConfig.Entry.newBuilder().setName("cluster_name").setStringValue(CassandraCluster.instance().getName()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("broadcast_address").setStringValue(executorMetadata.getIp()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("rpc_address").setStringValue(executorMetadata.getIp()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("listen_address").setStringValue(executorMetadata.getIp()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("storage_port").setLongValue(CassandraCluster.instance().getStoragePort()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("ssl_storage_port").setLongValue(CassandraCluster.instance().getSslStoragePort()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("native_transport_port").setLongValue(CassandraCluster.instance().getNativePort()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("rpc_port").setLongValue(CassandraCluster.instance().getRpcPort()))
                            .addVariables(TaskConfig.Entry.newBuilder().setName("seeds").setStringValue(
                                    // TODO find something better to figure out seeds (e.g. at least 3, max 5 or something like this)
                                    // Seeds should be chosen randomly - get more internals about order of offers
                                    Joiner.on(',').join(newArrayList(CassandraCluster.instance().seedsIpList()))))
                            .build();
                    final TaskDetails taskDetails = TaskDetails.newBuilder()
                        .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_RUN)
                        .setCassandraNodeRunTask(
                                CassandraNodeRunTask.newBuilder()
                                        .setVersion(cassandraVersion)
                                        .addAllCommand(newArrayList("apache-cassandra-" + cassandraVersion + "/bin/cassandra", "-p", "cassandra.pid"))
                                        .setTaskConfig(taskConfig)
                        )
                            .build();
                    final TaskID taskId = taskId(executorID.getValue() + ".server");
                    final TaskInfo task = TaskInfo.newBuilder()
                        .setName(taskId.getValue())
                        .setTaskId(taskId)
                        .setSlaveId(offer.getSlaveId())
                        .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                        .addAllResources(newArrayList(
                                cpu(cpuCores),
                                mem(memMb),
                                disk(diskMb),
                                ports(defaultCassandraPortMappings.values())
                        ))
                        .setExecutor(info)
                        .build();
                    LOGGER.debug(marker, "Launching CASSANDRA_NODE_RUN task = {}", protoToString(task));
                    driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
                    addSuperTask(new SuperTask(offer.getHostname(), task, info, taskDetails));
                    offerUsed = true;
                } else if (shouldRunHealthCheck(executorID)) {
                    ExecutorMetadata executorMetadata = CassandraCluster.instance().executorMetadata(executorID);
                    executorMetadata.updateHostname(offer.getHostname());

                    final TaskID serverTaskId = head.getTaskInfo().getTaskId();
                    final TaskID taskId = taskId(serverTaskId.getValue() + HEALTHCHECK_SUFFIX);
                    final TaskDetails taskDetails = TaskDetails.newBuilder()
                        .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_HEALTH_CHECK)
                        .setCassandraNodeHealthCheckTask(
                                CassandraNodeHealthCheckTask.newBuilder()
                                        .setJmx(jmxConnect(executorMetadata))
                                        .build()
                        )
                        .build();
                    final TaskInfo task = TaskInfo.newBuilder()
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
                    LOGGER.debug(marker, "Launching CASSANDRA_NODE_HEALTH_CHECK task", protoToString(task));
                    driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
                    addSuperTask(new SuperTask(offer.getHostname(), task, info, taskDetails));
                    offerUsed = true;
                } else if (CassandraCluster.instance().shouldStartRepairOnExecutor(executorID)) {
                    final TaskID serverTaskId = head.getTaskInfo().getTaskId();
                    final TaskID taskId = taskId(serverTaskId.getValue() + REPAIR_SUFFIX);
                    final TaskDetails taskDetails = TaskDetails.newBuilder()
                            .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_REPAIR)
                            .setCassandraNodeRepairTask(CassandraNodeRepairTask.newBuilder()
                                    .setJmx(jmxConnect(executorID)))
                            .build();
                    final TaskInfo task = TaskInfo.newBuilder()
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
                    LOGGER.debug(marker, "Launching CASSANDRA_NODE_REPAIR task", protoToString(task));
                    driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
                    addSuperTask(new SuperTask(offer.getHostname(), task, info, taskDetails));

                    offerUsed = true;
                } else if (CassandraCluster.instance().shouldGetRepairStatusOnExecutor(executorID)) {
                    final TaskID serverTaskId = head.getTaskInfo().getTaskId();
                    final TaskID taskId = taskId(serverTaskId.getValue() + REPAIR_STATUS_SUFFIX);
                    final TaskDetails taskDetails = TaskDetails.newBuilder()
                            .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_REPAIR_STATUS)
                            .build();
                    final TaskInfo task = TaskInfo.newBuilder()
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
                    LOGGER.debug(marker, "Launching CASSANDRA_NODE_REPAIR_STATUS task", protoToString(task));
                    driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
                    addSuperTask(new SuperTask(offer.getHostname(), task, info, taskDetails));

                    offerUsed = true;
                }
            }
        }

        if (!offerUsed) {
            LOGGER.trace(marker, "Declining Offer: {}", offer.getId().getValue());
            driver.declineOffer(offer.getId());
        }
        LOGGER.trace(marker, "< evaluateOffer(driver : {}, offer : {})", driver, protoToString(offer));
        return offerUsed;
    }

    private static JmxConnect jmxConnect(ExecutorID executorID) {
        return jmxConnect(CassandraCluster.instance().executorMetadata(executorID));
    }

    private static JmxConnect jmxConnect(ExecutorMetadata executorMetadata) {
        return JmxConnect.newBuilder()
                .setJmxPort(CassandraCluster.instance().getJmxPort())
                        // TODO add jmxSsl, jmxUsername, jmxPassword
                .setIp(executorMetadata.getIp())
                .build();
    }

    private boolean maybeLaunchExecutor(final SchedulerDriver driver, final Offer offer, final Marker marker, final ListMultimap<ExecutorID, SuperTask> tasksByExecutor) {
        final int ec = tasksByExecutor.size();
        final FluentIterable<SuperTask> tasksAlreadyOnHost = from(superTasks()).filter(SuperTask.hostnameEq(offer.getHostname()));
        if (ec < numberOfNodes && tasksAlreadyOnHost.isEmpty()) {
            final ExecutorID executorId = executorId(frameworkName + ".node." + execCounter.getAndIncrement() + ".executor");

            String osName = Env.option("OS_NAME").or(JreLoader.osFromSystemProperty());
            String javaExec = "macosx".equals(osName)
                    ? "$(pwd)/jre*/Contents/Home/bin/java"
                    : "$(pwd)/jre*/bin/java";

            final ExecutorInfo info = executorInfo(
                executorId,
                executorId.getValue(),
                frameworkName,
                commandInfo(
                    javaExec + " $JAVA_OPTS -classpath cassandra-executor.jar io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor",
                    environmentFromMap(executorEnv),
                    commandUri(getUrlForResource("/jre-" + osName + ".tar.gz"), true),
                    commandUri(getUrlForResource("/apache-cassandra-" + cassandraVersion + "-bin.tar.gz"), true),
                    commandUri(getUrlForResource("/cassandra-executor.jar"))
                ),
                cpu(0.1),
                mem(256),
                disk(16)
            );

            final TaskDetails taskDetails = TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.SLAVE_METADATA)
                .setSlaveMetadataTask(SlaveMetadataTask.newBuilder())
                .build();
            final TaskID taskId = taskId(executorId.getValue());
            final TaskInfo task = TaskInfo.newBuilder()
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
            LOGGER.debug(marker, "Launching executor = {}", protoToString(info));
            LOGGER.debug(marker, "Launching SLAVE_METADATA task = {}", protoToString(task));
            driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(task));
            addSuperTask(new SuperTask(offer.getHostname(), task, info, taskDetails));
            return true;
        }
        return false;
    }

    private boolean shouldRunHealthCheck(final ExecutorID executorID) {
        final Optional<Instant> previousHealthCheckTime = Optional.fromNullable(CassandraCluster.instance().executorMetadata(executorID).getLastHealthCheck());
        if (previousHealthCheckTime.isPresent()) {
            final Duration duration = new Duration(previousHealthCheckTime.get(), clock.now());
            return duration.isLongerThan(healthCheckInterval);
        } else {
            return true;
        }
    }

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

    @NotNull
    private static List<String> hasResources(
        @NotNull final Offer offer,
        final double cpu,
        final long mem,
        final long disk,
        @NotNull final Map<String, Long> portMapping
    ) {
        final List<String> errors = newArrayList();
        final ListMultimap<String, Resource> index = from(offer.getResourcesList()).index(resourceToName());
        final Double availableCpus = resourceValueDouble(headOption(index.get("cpus"))).or(0.0);
        final Long availableMem = resourceValueLong(headOption(index.get("mem"))).or(0L);
        final Long availableDisk = resourceValueLong(headOption(index.get("disk"))).or(0L);
        if (availableCpus <= cpu) {
            errors.add(String.format("Not enough cpu resources. Required %f only %f available.", cpu, availableCpus));
        }
        if (availableMem <= mem) {
            errors.add(String.format("Not enough mem resources. Required %d only %d available", mem, availableMem));
        }
        if (availableDisk <= disk) {
            errors.add(String.format("Not enough disk resources. Required %d only %d available", disk, availableDisk));
        }

        final TreeSet<Long> ports = resourceValueRange(headOption(index.get("ports")));
        for (final Map.Entry<String, Long> entry : portMapping.entrySet()) {
            final String key = entry.getKey();
            final Long value = entry.getValue();
            if (!ports.contains(value)) {
                errors.add(String.format("Unavailable port %d(%s). %d other ports available.", value, key, ports.size()));
            }
        }
        return errors;
    }

}
