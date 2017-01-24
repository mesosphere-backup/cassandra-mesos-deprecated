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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.util.Clock;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import static io.mesosphere.mesos.util.CassandraFrameworkProtosUtils.*;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public final class CassandraScheduler implements Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraScheduler.class);

    private static final Joiner JOIN_WITH_SPACE = Joiner.on(" ").skipNulls();

    private static final Pattern mesosVersionPattern = Pattern
        .compile("^(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<patch>\\d+)");

    private static final Function<FileDownload, CommandInfo.URI> uriToCommandInfoUri = new Function<FileDownload, CommandInfo.URI>() {
        @Override
        public CommandInfo.URI apply(final FileDownload input) {
            return commandUri(input.getDownloadUrl(), input.getExtract());
        }
    };

    private boolean reservationSupported;

    @NotNull
    private final PersistedCassandraFrameworkConfiguration configuration;
    @NotNull
    private final CassandraCluster cassandraCluster;
    @NotNull
    private final Clock clock;
    @NotNull
    private String principal;

    public CassandraScheduler(
        @NotNull final PersistedCassandraFrameworkConfiguration configuration,
        @NotNull final CassandraCluster cassandraCluster,
        @NotNull final Clock clock,
        @NotNull final String principal
    ) {
        this.configuration = configuration;
        this.cassandraCluster = cassandraCluster;
        this.clock = clock;
        this.principal = principal;
    }

    @Override
    public void registered(final SchedulerDriver driver, final FrameworkID frameworkId, final MasterInfo masterInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("> registered(driver : {}, frameworkId : {}, masterInfo : {})", driver, protoToString(frameworkId), protoToString(masterInfo));
        }
        configuration.frameworkId(frameworkId.getValue());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("< registered(driver : {}, frameworkId : {}, masterInfo : {})", driver, protoToString(frameworkId), protoToString(masterInfo));
        }

        detectDynamicReservationsSupport(masterInfo);
    }

    @Override
    public void reregistered(final SchedulerDriver driver, final MasterInfo masterInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("reregistered(driver : {}, masterInfo : {})", driver, protoToString(masterInfo));
        }

        detectDynamicReservationsSupport(masterInfo);
        driver.reconcileTasks(Collections.<TaskStatus>emptySet());
    }

    @Override
    public void resourceOffers(final SchedulerDriver driver, final List<Offer> offers) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("> resourceOffers(driver : {}, offers : {})", driver, protoToString(offers));
        }

        for (final Offer offer : offers) {
            final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
            final boolean offerUsed = evaluateOffer(driver, marker, offer);
            if (!offerUsed) {
                LOGGER.trace(marker, "Declining Offer: {}", offer.getId().getValue());
                driver.declineOffer(offer.getId());
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("< resourceOffers(driver : {}, offers : {})", driver,
                protoToString(offers));
        }
    }

    @Override
    public void offerRescinded(final SchedulerDriver driver, final OfferID offerId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("offerRescinded(driver : {}, offerId : {})", driver,
                protoToString(offerId));
        }
    }

    @Override
    public void statusUpdate(final SchedulerDriver driver, final TaskStatus status) {
        final Marker taskIdMarker = MarkerFactory.getMarker("taskId:" + status.getTaskId().getValue());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(taskIdMarker, "> statusUpdate(driver : {}, status : {})", driver, protoToString(status));
        }
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
                    // TODO really interested in staging state?
                    break;
                case TASK_STARTING:
                    // TODO really interested in starting state?
                    break;
                case TASK_RUNNING:
                    switch (statusDetails.getStatusDetailsType()) {
                        case NULL_DETAILS:
                            break;
                        case EXECUTOR_METADATA:
                            final ExecutorMetadata executorMetadata = statusDetails.getExecutorMetadata();
                            cassandraCluster.addExecutorMetadata(executorMetadata);
                            break;
                        case CASSANDRA_SERVER_RUN:
                            cassandraCluster.updateCassandraProcess(executorId, statusDetails.getCassandraServerRunMetadata());
                            break;
                        case HEALTH_CHECK_DETAILS:
                            break;
                        case ERROR_DETAILS:
                            break;
                    }
                    break;
                case TASK_FAILED:
                case TASK_KILLED:
                case TASK_LOST:
                case TASK_ERROR:
                    LOGGER.error(taskIdMarker, "Got status {} for task {}, executor {} ({}, healthy={}): {}",
                            status.getState(),
                            status.getTaskId().getValue(),
                            status.getExecutorId().getValue(),
                            protoToString(status.getReason()),
                            status.getHealthy(),
                            status.getMessage());
                case TASK_FINISHED:
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
                        switch (statusDetails.getStatusDetailsType()) {
                            case NULL_DETAILS:
                                break;
                            case EXECUTOR_METADATA:
                                break;
                            case ERROR_DETAILS:
                                LOGGER.error(taskIdMarker, protoToString(statusDetails.getSlaveErrorDetails()));
                                break;
                            case HEALTH_CHECK_DETAILS:
                                cassandraCluster.recordHealthCheck(executorId.getValue(), statusDetails.getHealthCheckDetails());
                                break;
                            case NODE_JOB_STATUS:
                                cassandraCluster.onNodeJobStatus(statusDetails);
                                break;
                        }
                        cassandraCluster.removeTask(taskId.getValue(), status);
                    }
                    break;
            }
        } catch (final InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task status data to type: " + SlaveStatusDetails.class.getName();
            LOGGER.error(msg, e);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(taskIdMarker, "< statusUpdate(driver : {}, status : {})", driver, protoToString(status));
        }
    }

    @Override
    public void frameworkMessage(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final byte[] data) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frameworkMessage(driver : {}, executorId : {}, slaveId : {}, data : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(data));
        }

        try {
            final SlaveStatusDetails statusDetails = SlaveStatusDetails.parseFrom(data);
            switch (statusDetails.getStatusDetailsType()) {
                case HEALTH_CHECK_DETAILS:
                    cassandraCluster.recordHealthCheck(executorId.getValue(), statusDetails.getHealthCheckDetails());
                    break;
                case NODE_JOB_STATUS:
                    cassandraCluster.onNodeJobStatus(statusDetails);
                    break;
            }
        } catch (final InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task status data to type: " + SlaveStatusDetails.class.getName();
            LOGGER.error(msg, e);
        }
    }

    @Override
    public void disconnected(final SchedulerDriver driver) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("disconnected(driver : {})", driver);
        }
        // TODO implement
    }

    @Override
    public void slaveLost(final SchedulerDriver driver, final SlaveID slaveId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("slaveLost(driver : {}, slaveId : {})", driver, protoToString(slaveId));
        }
        // TODO implement
    }

    @Override
    public void executorLost(final SchedulerDriver driver, final ExecutorID executorId, final SlaveID slaveId, final int status) {
        final Marker executorIdMarker = MarkerFactory.getMarker(
            "executorId:" + executorId.getValue());
        // this method will never be called by mesos until MESOS-313 is fixed
        // https://issues.apache.org/jira/browse/MESOS-313
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(executorIdMarker, "executorLost(driver : {}, executorId : {}, slaveId : {}, status : {})", driver, protoToString(executorId), protoToString(slaveId), protoToString(status));
        }
        cassandraCluster.removeExecutor(executorId.getValue());
    }

    @Override
    public void error(final SchedulerDriver driver, final String message) {
        LOGGER.error("error(driver : {}, message : {})", driver, message);
    }

    // ---------------------------- Helper methods ---------------------------------------------------------------------

    /**
     * @return boolean representing if the the offer was used
     */
    private boolean evaluateOffer(@NotNull final SchedulerDriver driver, @NotNull final Marker marker, @NotNull final Offer offer) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "> evaluateOffer(driver : {}, offer : {})", protoToString(offer));
        }

        final Optional<TaskResources> reserveResourcesForHost = cassandraCluster.shouldCreateReservation(
            marker, offer);
        if (reservationSupported && reserveResourcesForHost.isPresent()) {
            final TaskResources res = reserveResourcesForHost.get();
            LOGGER.debug("{}", res.getPortsList());
            final List<Resource> resources = newArrayList(
                reserveCpu(res.getCpuCores(), configuration.mesosRole(), principal),
                reserveMem(res.getMemMb(), configuration.mesosRole(), principal),
                reserveDisk(res.getDiskMb(), configuration.mesosRole(), principal),
                reservePorts(res.getPortsList(), configuration.mesosRole(), principal)
            );
            final Offer.Operation reservation = Offer.Operation.newBuilder()
                .setType(Offer.Operation.Type.RESERVE)
                .setReserve(
                    Offer.Operation.Reserve.newBuilder()
                        .addAllResources(resources)
                        .build()
                )
                .build();
            LOGGER.debug("Reservation: {}", protoToString(reservation));
            driver.acceptOffers(Collections.singletonList(offer.getId()),
                Collections.singletonList(reservation), Filters.getDefaultInstance());

            final long reservedNodesCount = cassandraCluster.getClusterState().reservedNodesCount();
            cassandraCluster.getClusterState().updateReservedNodesCount(reservedNodesCount + 1);

            return true;
        }

        final TasksForOffer tasksForOffer = cassandraCluster.getTasksForOffer(offer);
        if (tasksForOffer == null || !tasksForOffer.hasAnyTask()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "< evaluateOffer(driver : {}, offer : {}) = nothing to do", protoToString(offer));
            }
            return false;
        }

        final CassandraNodeExecutor executor = tasksForOffer.getExecutor();
        final ExecutorID executorId = executorId(executor.getExecutorId());

        // process tasks to kill
        for (final TaskID taskID : tasksForOffer.getKillTasks()) {
            //Trying to unreserve resources if dynamic reservations are enabled
            if (reservationSupported && configuration.isReserveRequired()) {
                final Optional<CassandraNode> nodeForTask = cassandraCluster.getNodeForTask(
                    taskID.getValue());
                if (nodeForTask.isPresent()) {
                    final TaskResources taskResources = nodeForTask.get().getCassandraNodeExecutor()
                        .getResources();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(marker,
                            "Unreserving resources for task {} on executor {}, slave {}: {}",
                            protoToString(taskID), protoToString(executorId),
                            protoToString(offer.getSlaveId()),
                            protoToString(taskResources)
                        );
                    }
                    final List<Resource> resources = newArrayList(
                        reserveCpu(taskResources.getCpuCores(), configuration.mesosRole(),
                            principal),
                        reserveMem(taskResources.getMemMb(), configuration.mesosRole(), principal),
                        reserveDisk(taskResources.getDiskMb(), configuration.mesosRole(),
                            principal),
                        reservePorts(taskResources.getPortsList(), configuration.mesosRole(),
                            principal)
                    );
                    final Offer.Operation reservation = Offer.Operation.newBuilder()
                        .setType(Offer.Operation.Type.UNRESERVE)
                        .setUnreserve(
                            Offer.Operation.Unreserve.newBuilder()
                                .addAllResources(resources)
                                .build()
                        )
                        .build();
                    driver.acceptOffers(Collections.singletonList(offer.getId()),
                        Collections.singletonList(reservation), Filters.getDefaultInstance());

                    final long reservedNodesCount = cassandraCluster.getClusterState().reservedNodesCount();
                    cassandraCluster.getClusterState().updateReservedNodesCount(reservedNodesCount - 1);
                }

            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "killing task {} on executor {}, slave {}",
                    protoToString(taskID), protoToString(executorId),
                    protoToString(offer.getSlaveId()));
            }
            driver.killTask(taskID);
        }

        // process tasks to submit as framework message
        for (final TaskDetails taskDetails : tasksForOffer.getSubmitTasks()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "submitting framework message to executor {}, slave {} : {}", protoToString(executorId), protoToString(offer.getSlaveId()), protoToString(taskDetails));
            }
            driver.sendFrameworkMessage(executorId, offer.getSlaveId(), taskDetails.toByteArray());
        }

        if (tasksForOffer.getLaunchTasks().isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "< evaluateOffer(driver : {}, offer : {}) = no tasks to launch", protoToString(offer));
            }
            // no tasks to launch
            return false;
        }

        final List<TaskInfo> taskInfos = newArrayList();

        for (final CassandraNodeTask cassandraNodeTask : tasksForOffer.getLaunchTasks()) {
            final TaskDetails taskDetails = cassandraNodeTask.getTaskDetails();

            final ExecutorInfo info = executorInfo(
                executorId,
                executorId.getValue(),
                executor.getSource(),
                commandInfo(
                    JOIN_WITH_SPACE.join(executor.getCommandList()),
                    environmentFromTaskEnv(executor.getTaskEnv()),
                    newArrayList(FluentIterable.from(executor.getDownloadList()).transform(uriToCommandInfoUri))
                ),
                resourceList(executor.getResources(), configuration.mesosRole(), principal)
            );

            final TaskID taskId = taskId(cassandraNodeTask.getTaskId());
            final List<Resource> resources = resourceList(cassandraNodeTask.getResources(), configuration.mesosRole(), principal);

            final TaskInfo task = TaskInfo.newBuilder()
                .setName(getTaskName(cassandraNodeTask.getTaskName(), taskId.getValue()))
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setData(ByteString.copyFrom(taskDetails.toByteArray()))
                .addAllResources(resources)
                .setExecutor(info)
                .build();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "Launching task {} in executor {}. Details = {}", taskDetails.getType(), executorId.getValue(), protoToString(task));
            }
            taskInfos.add(task);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "< evaluateOffer(driver : {}, offer : {}) = {}", protoToString(offer), protoToString(taskInfos));
        }

        driver.launchTasks(Collections.singleton(offer.getId()), taskInfos);

        return true;
    }

    private void detectDynamicReservationsSupport(MasterInfo masterInfo) {
        final Matcher matcher = mesosVersionPattern.matcher(masterInfo.getVersion());
        if (matcher.find()) {
            int majorVersion = Integer.parseInt(matcher.group("major"));
            int minorVersion = Integer.parseInt(matcher.group("minor"));
            reservationSupported =
                majorVersion > 0 || (majorVersion == 0 && minorVersion >= 24);
        } else {
            reservationSupported = false;
        }
        LOGGER.info(
            String.format("Dynamic reservations support: %b", this.reservationSupported));
    }

    @NotNull
    public static String getTaskName(@Nullable final String taskName, @NotNull final String taskIdValue) {
        if (taskName == null || taskName.trim().isEmpty()) {
            return taskIdValue;
        } else {
            return taskName;
        }
    }

    @NotNull
    @VisibleForTesting
    static List<Resource> resourceList(@NotNull final TaskResources taskResources, @NotNull final String role, String principal) {
        final List<Resource> retVal = newArrayList(
            reserveCpu(taskResources.getCpuCores(), role, principal),
            reserveMem(taskResources.getMemMb(), role, principal)
        );
        if (taskResources.hasDiskMb() && taskResources.getDiskMb() > 0) {
            retVal.add(reserveDisk(taskResources.getDiskMb(), role, principal));
        }
        return retVal;
    }

    @NotNull
    @VisibleForTesting
    static List<Resource> ports(@NotNull final Iterable<Long> ports, @NotNull final String mesosRole, String principal, Offer offer) {
        final ListMultimap<String, Resource> resourcesForRoleAndOffer = resourcesForRoleAndOffer(mesosRole, offer);

        ImmutableMap<String, Collection<Long>> portsByRole = from(ports)
                .index(byRole(resourcesForRoleAndOffer))
                .asMap();

        return from(portsByRole.entrySet())
                .transform(roleAndPortsToResource(principal)).toList();

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