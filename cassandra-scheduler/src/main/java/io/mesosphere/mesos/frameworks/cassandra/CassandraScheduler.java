package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.http.HttpServer;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Predicates.not;
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
        tuple2("storage_port_ssl", 7001L),
        tuple2("jmx", 7199L),
        tuple2("native_transport_port", 9042L),
        tuple2("rpc_port", 9160L)
    );

    @NotNull
    private final ScheduledExecutorService scheduledExecutorService;
    @NotNull
    private final Map<ExecutorID, SlaveMetadata> executorMetadata;
    @NotNull
    private final AtomicInteger execCounter;
    @NotNull
    private final AtomicInteger taskCounter;
    @NotNull
    private final String frameworkName;
    @NotNull
    private final HttpServer httpServer;
    private final int numberOfNodes;
    private final double cpuCores;
    private final long memMb;
    private final long diskMb;

    // TODO(BenWhitehead): Fix thread safety for this state
    @NotNull
    private List<SuperTask> superTasks;

    @NotNull
    private final Map<String, String> executorEnv;

    public CassandraScheduler(
        @NotNull final String frameworkName,
        @NotNull final HttpServer httpServer,
        final int numberOfNodes,
        final double cpuCores,
        final long memMb,
        final long diskMb
    ) {
        this.frameworkName = frameworkName;
        this.httpServer = httpServer;
        this.numberOfNodes = numberOfNodes;
        this.cpuCores = cpuCores;
        this.memMb = memMb;
        this.diskMb = diskMb;

        taskCounter = new AtomicInteger(0);
        execCounter = new AtomicInteger(0);
        executorMetadata = Maps.newConcurrentMap();
        superTasks = Collections.synchronizedList(Lists.<SuperTask>newArrayList());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        executorEnv = Collections.unmodifiableMap(newHashMap("JAVA_OPTS", "-Xms256m -Xmx256m"));
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
            final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
            LOGGER.debug(marker, "> Evaluating offer");
            boolean offerUsed = false;
            final ImmutableListMultimap<ExecutorID, SuperTask> tasksByExecutor = from(superTasks).index(SuperTask.toExecutorId());
            final int ec = tasksByExecutor.size();

            final FluentIterable<SuperTask> tasksAlreadyOnHost = from(superTasks).filter(SuperTask.hostnameEq(offer.getHostname()));
            if (ec < numberOfNodes && tasksAlreadyOnHost.isEmpty()) {
                final ExecutorID executorId = executorId(frameworkName + ".node." + execCounter.getAndIncrement() + ".executor");
                final ExecutorInfo info = executorInfo(
                    executorId,
                    executorId.getValue(),
                    frameworkName,
                    commandInfo(
                        "$(pwd)/jdk*/bin/java $JAVA_OPTS -classpath cassandra-executor.jar io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor",
                        environmentFromMap(executorEnv),
                        commandUri(httpServer.getUrlForResource("/jdk.tar.gz"), true),
                        commandUri(httpServer.getUrlForResource("/cassandra.tar.gz"), true),
                        commandUri(httpServer.getUrlForResource("/cassandra-executor.jar"))
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
                LOGGER.debug(marker, "launching task = {}", protoToString(task));
                driver.launchTasks(newArrayList(offer.getId()), newArrayList(task));
                superTasks.add(new SuperTask(offer.getHostname(), task, info, taskDetails));
                offerUsed = true;
            }

            if (offerUsed) {
                LOGGER.trace(marker, "< Evaluating offer");
                continue;
            }

            for (final ExecutorID executorID : offer.getExecutorIdsList()) {
                final ImmutableList<SuperTask> tasks = tasksByExecutor.get(executorID);
                final boolean nodeNotAlreadyRunning = from(tasks).filter(SuperTask.taskDetailsTypeEq(TaskDetails.TaskType.CASSANDRA_NODE_RUN)).isEmpty();
                if (!tasks.isEmpty() && nodeNotAlreadyRunning) {
                    final List<String> errors = hasResources(offer, cpuCores, memMb, diskMb, defaultCassandraPortMappings);
                    if (!errors.isEmpty()) {
                        LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                        continue;
                    }

                    final SuperTask head = tasks.get(0);
                    final ExecutorInfo info = head.getExecutorInfo();
                    final SlaveMetadata metadata = executorMetadata.get(executorID);
                    if (info != null && metadata != null) {
                        final TaskDetails taskDetails = TaskDetails.newBuilder()
                            .setTaskType(TaskDetails.TaskType.CASSANDRA_NODE_RUN)
                            .setCassandraNodeRunTask(
                                CassandraNodeRunTask.newBuilder()
                                    .addAllCommand(newArrayList("apache-cassandra-2.1.2/bin/cassandra", "-p", "cassandra.pid"))  //TODO(BenWhitehead) Cleanup path handling to make more maintainable across different versions of cassandra
                                    .addTaskFiles(
                                        TaskFile.newBuilder()
                                            .setOutputPath("apache-cassandra-2.1.2/conf/cassandra.yaml") //TODO(BenWhitehead) Cleanup path handling to make more maintainable across different versions of cassandra
                                            .setData(
                                                ByteString.copyFromUtf8(
                                                    CassandraYaml.defaultCassandraYaml()
                                                        .setClusterName(frameworkName)
                                                        .setRpcAddress(metadata.getIp())
                                                        .setListenAddress(metadata.getIp())
                                                        .setStoragePort(defaultCassandraPortMappings.get("storage_port"))
                                                        .setStoragePortSsl(defaultCassandraPortMappings.get("storage_port_ssl"))
                                                        .setNativeTransportPort(defaultCassandraPortMappings.get("native_transport_port"))
                                                        .setRpcPort(defaultCassandraPortMappings.get("rpc_port"))
                                                        // TODO(BenWhitehead): Figure out how to set the JMX port
                                                        .setSeeds(newArrayList(from(executorMetadata.values()).transform(toIp)))
                                                        .dump()
                                                )
                                            )
                                    )
                            )
                            .build();
                        final TaskID taskId = taskId(executorID.getValue() + ".task." + taskCounter.getAndIncrement());
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
                        driver.launchTasks(newArrayList(offer.getId()), newArrayList(task));
                        superTasks.add(new SuperTask(offer.getHostname(), task, info, taskDetails));
                        offerUsed = true;
                    }
                }
                if (offerUsed) {
                    break;
                }
            }

            if (!offerUsed) {
                scheduledExecutorService.schedule(new DeclineOffer(driver, offer), 1, TimeUnit.SECONDS);
            }
            LOGGER.trace(marker, "< Evaluating offer");
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
            switch (status.getState()) {
                case TASK_STAGING:
                case TASK_STARTING:
                case TASK_RUNNING:
                    switch (statusDetails.getStatusDetailsType()) {
                        case NULL_DETAILS:
                            break;
                        case SLAVE_METADATA:
                            final SlaveMetadata slaveMetadata = statusDetails.getSlaveMetadata();
                            executorMetadata.put(executorId, slaveMetadata);
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
                            from(superTasks).filter(SuperTask.taskIdEq(taskId)).transform(SuperTask.toExecutorId())
                        ).or(executorId);
                        executorLost(driver, executorIdForTask, status.getSlaveId(), status.getState().ordinal());
                    } else {
                        superTasks = filterNot(superTasks, SuperTask.taskIdEq(taskId));
                        executorMetadata.remove(executorId);
                        switch (statusDetails.getStatusDetailsType()) {
                            case NULL_DETAILS:
                                break;
                            case SLAVE_METADATA:
                                break;
                            case ERROR_DETAILS:
                                LOGGER.error(taskIdMarker, protoToString(statusDetails.getSlaveErrorDetails()));
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
        superTasks = filterNot(superTasks, SuperTask.executorIdEq(executorId));
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    private static <A> List<A> filterNot(@NotNull final List<A> list, @NotNull final Predicate<A> predicate) {
        return Collections.synchronizedList(newArrayList(from(list).filter(not(predicate))));
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


    private static final class DeclineOffer implements Runnable {
        private final SchedulerDriver driver;
        private final Offer offer;

        public DeclineOffer(final SchedulerDriver driver, final Offer offer) {
            this.driver = driver;
            this.offer = offer;
        }

        @Override
        public void run() {
            LOGGER.debug("driver.declineOffer({})", protoToString(offer.getId()));
            driver.declineOffer(offer.getId());
        }
    }

    private static final Function<SlaveMetadata, String> toIp = new Function<SlaveMetadata, String>() {
        @Override
        public String apply(final SlaveMetadata input) {
            return input.getIp();
        }
    };

}
