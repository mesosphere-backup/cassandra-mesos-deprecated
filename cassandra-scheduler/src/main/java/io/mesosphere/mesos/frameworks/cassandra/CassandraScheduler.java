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
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    private static final Joiner JOIN_WITH_SPACE = Joiner.on(" ").skipNulls();

    private static final Function<URI, CommandInfo.URI> uriToCommandInfoUri = new Function<URI, CommandInfo.URI>() {
        @Override
        public CommandInfo.URI apply(final URI input) {
            return commandUri(input.getValue(), input.getExtract());
        }
    };

    @NotNull
    private final CassandraCluster cassandraCluster;

    public CassandraScheduler(@NotNull final CassandraCluster cassandraCluster) {
        this.cassandraCluster = cassandraCluster;
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
            final List<TaskInfo> tasksToLaunch = evaluateOffer(MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname()), offer);
            final boolean offerUsed = !tasksToLaunch.isEmpty();
            if (offerUsed) {
                driver.launchTasks(newArrayList(offer.getId()), tasksToLaunch);
            } else {
                LOGGER.trace(marker, "Declining Offer: {}", offer.getId().getValue());
                driver.declineOffer(offer.getId());
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
                        case EXECUTOR_METADATA:
                            final ExecutorMetadata executorMetadata = statusDetails.getExecutorMetadata();
                            cassandraCluster.addExecutorMetadata(executorMetadata);
                            break;
                        case ERROR_DETAILS:
                            break;
                        case HEALTH_CHECK_DETAILS:
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
                        final Optional<String> opt = cassandraCluster.getExecutorIdForTask(taskId.getValue());
                        final ExecutorID executorIdForTask;
                        if (opt.isPresent()) {
                            executorIdForTask = executorId(opt.get());
                        } else {
                            executorIdForTask = executorId;
                        }
                        executorLost(driver, executorIdForTask, status.getSlaveId(), status.getState().ordinal());
                    } else {
                        cassandraCluster.removeTask(taskId.getValue());
                        cassandraCluster.removeExecutorMetadata(executorId.getValue());
                        switch (statusDetails.getStatusDetailsType()) {
                            case NULL_DETAILS:
                                break;
                            case EXECUTOR_METADATA:
                                break;
                            case ERROR_DETAILS:
                                LOGGER.error(taskIdMarker, protoToString(statusDetails.getSlaveErrorDetails()));
                                break;
                            case HEALTH_CHECK_DETAILS:
                                final CassandraNodeHealthCheckDetails healthCheckDetails = statusDetails.getCassandraNodeHealthCheckDetails();
                                cassandraCluster.recordHealthCheck(executorId.getValue(), healthCheckDetails);
                                break;
                        }
                    }
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task status data to type: " + SlaveStatusDetails.class.getName();
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
        cassandraCluster.removeExecutor(executorId.getValue());
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    // ---------------------------- Helper methods ---------------------------------------------------------------------

    /**
     * @return boolean representing if the the offer was used
     */
    private List<TaskInfo> evaluateOffer(@NotNull final Marker marker, @NotNull final Offer offer) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "> evaluateOffer(driver : {}, offer : {})", protoToString(offer));
        }

        final List<TaskInfo> tasksToLaunch = newArrayList();

        final Optional<Tuple2<CassandraNodeExecutor, List<CassandraNodeTask>>> executorOption = cassandraCluster.getTasksForOffer(offer);
        if (executorOption.isPresent()) {
            final CassandraNodeExecutor executor = executorOption.get()._1;
            final List<CassandraNodeTask> tasksForOffer = executorOption.get()._2;
            for (final CassandraNodeTask cassandraNodeTask : tasksForOffer) {
                final ExecutorID executorId = executorId(executor.getExecutorId());
                final List<String> fullCommand = newArrayList(executor.getCommand());
                fullCommand.addAll(executor.getCommandArgsList());
                final ExecutorInfo info = executorInfo(
                    executorId,
                    executorId.getValue(),
                    executor.getSource(),
                    commandInfo(
                        JOIN_WITH_SPACE.join(fullCommand),
                        environmentFromTaskEnv(executor.getTaskEnv()),
                        newArrayList(from(executor.getResourceList()).transform(uriToCommandInfoUri))
                    ),
                    cpu(executor.getCpuCores()),
                    mem(executor.getMemMb()),
                    disk(executor.getDiskMb())
                );

                final TaskDetails taskDetails = cassandraNodeTask.getTaskDetails();

                final TaskID taskId = taskId(cassandraNodeTask.getTaskId());
                final List<Resource> resources = newArrayList(
                    cpu(cassandraNodeTask.getCpuCores()),
                    mem(cassandraNodeTask.getMemMb()),
                    disk(cassandraNodeTask.getDiskMb())
                );
                if (!cassandraNodeTask.getPortsList().isEmpty()) {
                    resources.add(ports(cassandraNodeTask.getPortsList()));
                }
                final TaskInfo task = TaskInfo.newBuilder()
                    .setName(taskId.getValue())
                    .setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId())
                    .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                    .addAllResources(resources)
                    .setExecutor(info)
                    .build();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(marker, "Launching task {} in executor {}. Details = {}", taskDetails.getTaskType(), cassandraNodeTask.getExecutorId(), protoToString(task));
                }
                tasksToLaunch.add(task);
            }
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(marker, "< evaluateOffer(driver : {}, offer : {}) = {}", protoToString(offer), protoToString(tasksToLaunch));
        }
        return tasksToLaunch;
    }

    @NotNull
    private static Environment environmentFromTaskEnv(@NotNull final TaskEnv taskEnv) {
        final Environment.Builder builder = Environment.newBuilder();
        for (final TaskEnv.Entry entry : taskEnv.getVariablesList()) {
            builder.addVariables(
                Environment.Variable.newBuilder()
                    .setName(entry.getName())
                    .setValue(entry.getValue())
                    .build()
            );
        }
        return builder.build();

    }

}
