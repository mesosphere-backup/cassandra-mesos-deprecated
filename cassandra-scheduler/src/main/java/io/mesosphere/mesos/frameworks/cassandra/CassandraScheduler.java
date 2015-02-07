package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.*;
import static io.mesosphere.mesos.util.Functions.headOption;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

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

    // TODO(BenWhitehead): Fix thread safety for this state
    @NotNull
    private List<SuperTask> superTasks;

    @NotNull
    private final Map<String, String> executorEnv;

    public CassandraScheduler(@NotNull final String frameworkName, @NotNull final HttpServer httpServer, final int numberOfNodes) {
        this.frameworkName = frameworkName;
        this.httpServer = httpServer;
        this.numberOfNodes = numberOfNodes;

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
            // TODO: Make this code smarter and able to launch multiple tasks for the same offer
            boolean offerUsed = false;
            final ImmutableListMultimap<ExecutorID, SuperTask> tasksByExecutor = from(superTasks).index(SuperTask.toExecutorId());
            final int ec = tasksByExecutor.size();

            final FluentIterable<SuperTask> tasksAlreadyOnHost = from(superTasks).filter(SuperTask.hostnameEq(offer.getHostname()));
            if (ec < numberOfNodes && tasksAlreadyOnHost.isEmpty()) {
                final ExecutorID executorId = executorId(frameworkName + ".node." + execCounter.getAndIncrement() + ".executor");
                final ExecutorInfo info = executorInfo(
                    executorId,
                    executorId.getValue(),
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
                LOGGER.debug("launching task = {}", protoToString(task));
                driver.launchTasks(newArrayList(offer.getId()), newArrayList(task));
                superTasks.add(new SuperTask(offer.getHostname(), task, info, taskDetails));
                offerUsed = true;
            }

            if (offerUsed) {
                continue;
            }

            for (final ExecutorID executorID : offer.getExecutorIdsList()) {
                final ImmutableList<SuperTask> tasks = tasksByExecutor.get(executorID);
                if (tasks != null && !tasks.isEmpty() && from(tasks).filter(SuperTask.taskDetailsTypeEq(TaskDetails.TaskType.CASSANDRA_NODE_RUN)).isEmpty()) {

                    final List<Resource> resourcesList = offer.getResourcesList();
                    final Double cpus = headOption(
                        from(resourcesList).filter(new Predicate<Resource>() {
                            @Override
                            public boolean apply(final Resource input) {
                                return "cpus".equals(input.getName());
                            }
                        }).transform(new Function<Resource, Double>() {
                            @Override
                            public Double apply(final Resource input) {
                                return input.getScalar().getValue();
                            }
                        })
                    ).or(1.0);
                    final Double mem = headOption(
                        from(resourcesList).filter(new Predicate<Resource>() {
                            @Override
                            public boolean apply(final Resource input) {
                                return "mem".equals(input.getName());
                            }
                        }).transform(new Function<Resource, Double>() {
                            @Override
                            public Double apply(final Resource input) {
                                return input.getScalar().getValue();
                            }
                        })
                    ).or(4d * 1024);

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
                                cpu(cpus - 0.1),
                                mem(mem - 256),
                                disk(2 * 1024)  // 2GB
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
        }
        LOGGER.trace("< resourceOffers(driver : {}, offers : {})", driver, protoToString(offers));
    }

    @Override
    public void offerRescinded(final SchedulerDriver driver, final OfferID offerId) {
        LOGGER.debug("offerRescinded(driver : {}, offerId : {})", driver, protoToString(offerId));
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
        final Marker taskIdMarker = MarkerFactory.getMarker(status.getTaskId().getValue());
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
        final Marker executorIdMarker = MarkerFactory.getMarker(executorId.getValue());
        // this method will never be called by mesos until MESOS-313 is fixed
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.debug(executorIdMarker, "executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(status));
        superTasks = filterNot(superTasks, SuperTask.executorIdEq(executorId));
    }

    @NotNull
    private <A> List<A> filterNot(@NotNull final List<A> list, @NotNull final Predicate<A> predicate) {
        return Collections.synchronizedList(newArrayList(from(list).filter(not(predicate))));
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    public static <K, V> Map<K, V> newHashMap(@NotNull final K key, @NotNull final V value) {
        final HashMap<K, V> map = Maps.newHashMap();
        map.put(key, value);
        return map;
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
