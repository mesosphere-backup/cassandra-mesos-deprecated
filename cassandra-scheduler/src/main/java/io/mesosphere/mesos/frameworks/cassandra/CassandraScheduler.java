package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    @NotNull
    private final ScheduledExecutorService scheduledExecutorService;
    @NotNull
    private final Map<ExecutorID, ExecutorInfo> executors;
    @NotNull
    private final Map<ExecutorID, Map<TaskID, TaskInfo>> tasksByExecutor;
    @NotNull
    private final AtomicInteger execCounter;
    @NotNull
    private final AtomicInteger taskCounter;
    @NotNull
    private final String frameworkName;

    public CassandraScheduler(@NotNull final String frameworkName) {
        this.frameworkName = frameworkName;
        taskCounter = new AtomicInteger(0);
        execCounter = new AtomicInteger(0);
        tasksByExecutor = Maps.newConcurrentMap();
        executors = Maps.newConcurrentMap();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
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
        LOGGER.debug("resourceOffers(driver : {}, offers : {})", driver, protoToString(offers));
        for (final Offer offer : offers) {
            // TODO: Make this code smarter and able to launch multiple tasks for the same offer
            boolean offerUsed = false;
            final int ec = executors.size();

            if (ec < 4) {
                final ExecutorID executorId = executorId(frameworkName + ".node." + execCounter.getAndIncrement() + ".executor");
                final ExecutorInfo info = executorInfo(
                        executorId,
                        executorId.getValue(),
                        commandInfo(
                                "java -classpath cassandra-executor-*-jar-with-dependencies.jar io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor",
                                "http://localhost:8998/cassandra-executor/target/cassandra-executor-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
                        ),
                        cpu(0.1),
                        mem(256),
                        disk(16)
                );

                final TaskID taskId = taskId(executorId.getValue());
                final TaskInfo task = TaskInfo.newBuilder()
                        .setName(taskId.getValue())
                        .setTaskId(taskId)
                        .setSlaveId(offer.getSlaveId())
                        .setData(ByteString.copyFromUtf8("keep-alive"))
                        .addAllResources(newArrayList(
                                cpu(0.1),
                                mem(16),
                                disk(16)
                        ))
                        .setExecutor(info)
                        .build();
                LOGGER.debug("launching task = {}", protoToString(task));
                driver.launchTasks(newArrayList(offer.getId()), newArrayList(task));
                executors.put(executorId, info);
                tasksByExecutor.put(executorId, newHashMap(taskId, task));
                offerUsed = true;
            }

            if (offerUsed) {
                continue;
            }

            for (final ExecutorID executorID : offer.getExecutorIdsList()) {
                final Map<TaskID, TaskInfo> tasks = getOrElse(tasksByExecutor, executorID, Maps.<TaskID, TaskInfo>newConcurrentMap());
                if (tasks.size() < 4) {
                    final ExecutorInfo info = executors.get(executorID);
                    if (info != null) {
                        final TaskID taskId = taskId(executorID.getValue() + ".task." + taskCounter.getAndIncrement());
                        final TaskInfo task = TaskInfo.newBuilder()
                                .setName(taskId.getValue())
                                .setTaskId(taskId)
                                .setSlaveId(offer.getSlaveId())
                                .addAllResources(newArrayList(
                                        cpu(0.25),
                                        mem(64),
                                        disk(1024)
                                ))
                                .setExecutor(info)
                                .build();
                        driver.launchTasks(newArrayList(offer.getId()), newArrayList(task));
                        tasks.put(taskId, task);
                        offerUsed = true;
                    }
                }
                tasksByExecutor.put(executorID, tasks);
                if (offerUsed) {
                    break;
                }
            }

            scheduledExecutorService.schedule(new DeclineOffer(driver, offer), 3, TimeUnit.SECONDS);
        }
    }

    @Override
    public void offerRescinded(final SchedulerDriver driver, final OfferID offerId) {
        LOGGER.debug("offerRescinded(driver : {}, offerId : {})", driver, protoToString(offerId));
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
//        LOGGER.debug("statusUpdate(driver : {}, status : {})", driver, protoToString(status));
        LOGGER.debug("statusUpdate(driver : {}, status : {})", driver, status);
        switch (status.getState()) {
            case TASK_STAGING:
            case TASK_STARTING:
            case TASK_RUNNING:
                break;
            case TASK_FINISHED:
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_LOST:
            case TASK_ERROR:
                final ExecutorID executorId = status.getExecutorId();
                final TaskID taskId = status.getTaskId();
                if (status.getSource() == TaskStatus.Source.SOURCE_SLAVE && status.getReason() == TaskStatus.Reason.REASON_EXECUTOR_TERMINATED) {
                    // this code should really be handled by executorLost, but it can't due to the fact that
                    // executorLost will never be called.

                    // there is the possibility that the executorId we get in the task status is empty,
                    // so here we use the taskId to lookup the executorId based on the tasks we're tracking
                    // to try and have a more accurate value.
                    ExecutorID executorIdForTask = executorId;
                    for (final Map.Entry<ExecutorID, Map<TaskID, TaskInfo>> entry : tasksByExecutor.entrySet()) {
                        if (entry.getValue().containsKey(taskId)) {
                            executorIdForTask = entry.getKey();
                        }
                    }
                    executorLost(driver, executorIdForTask, status.getSlaveId(), status.getState().ordinal());
                } else {
                    final Map<TaskID, TaskInfo> tasks = tasksByExecutor.get(executorId);
                    if (tasks != null) {
                        tasks.remove(taskId);
                        tasksByExecutor.put(executorId, tasks);
                    }
                }
                break;
        }
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
        // this method will never be called by mesos until MESOS-313 is fixed
        // https://issues.apache.org/jira/browse/MESOS-313
        LOGGER.debug("executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(status));
        executors.remove(executorId);
        tasksByExecutor.remove(executorId);
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    public static <K, V> V getOrElse(@NotNull final Map<K, V> map, @NotNull final K key, @NotNull final V otherwise) {
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            return otherwise;
        }
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

}
