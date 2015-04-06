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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Env;
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import io.mesosphere.mesos.util.Clock;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.CassandraFrameworkProtosUtils.*;
import static io.mesosphere.mesos.util.Functions.*;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

/**
 * The processing model that is used by Mesos is that of an actor system.
 *
 * This means that we're guaranteed to only have one thread calling a method in the scheduler at any one time.
 *
 * Design considerations:
 * <ol>
 *     <li>
 *         Mesos will soon have the concept of a dynamic reservation, this means that as a framework we can reserve
 *         resources for the framework. This will be leveraged to ensure that we receive resource offers for all hosts
 *         As part of the reservation process we will make sure to always reserve enough resources to ensure that we
 *         will receive an offer. This fact will allow us to not have to implement our own task time based scheduler.
 *     </li>
 *     <li>
 *         Periodic tasks will be defined with a time specifying when they are to be run. When a resource offer is
 *         received the time will be evaluated and if the current time is past when the task is scheduled to be run
 *         a task will be returned for the offer.
 *     </li>
 * </ol>
 */
public final class CassandraCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCluster.class);

    private static final Joiner JOINER = Joiner.on("','");
    private static final Joiner SEEDS_FORMAT_JOINER = Joiner.on(',');
    private static final Pattern URL_FOR_RESOURCE_REPLACE = Pattern.compile("(?<!:)/+");

    public static final String PORT_STORAGE = "storage_port";
    public static final String PORT_STORAGE_SSL = "ssl_storage_port";
    public static final String PORT_NATIVE = "native_transport_port";
    public static final String PORT_RPC = "rpc_port";
    public static final String PORT_JMX = "jmx_port";

    // see: http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
    private static final Map<String, Long> defaultPortMappings = unmodifiableHashMap(
        tuple2(PORT_STORAGE, 7000L),
        tuple2(PORT_STORAGE_SSL, 7001L),
        tuple2(PORT_JMX, 7199L),
        tuple2(PORT_NATIVE, 9042L),
        tuple2(PORT_RPC, 9160L)
    );

    private static final Map<String, String> executorEnv = unmodifiableHashMap(
        tuple2("JAVA_OPTS", "-Xms256m -Xmx256m")
    );

    @NotNull
    private final Clock clock;
    @NotNull
    private final String httpServerBaseUrl;

    @NotNull
    private final ExecutorCounter execCounter;
    @NotNull
    private final PersistedCassandraClusterState clusterState;
    @NotNull
    private final PersistedCassandraClusterHealthCheckHistory healthCheckHistory;
    @NotNull
    private final PersistedCassandraFrameworkConfiguration configuration;
    @NotNull
    private final PersistedCassandraClusterJobs jobsState;

    @NotNull
    private final Map<ClusterJobType, ClusterJobHandler> clusterJobHandlers;

    public static int getPortMapping(CassandraFrameworkConfiguration configuration, String name) {
        for (PortMapping portMapping : configuration.getPortMappingList()) {
            if (portMapping.getName().equals(name)) {
                return portMapping.getPort();
            }
        }
        Long port = defaultPortMappings.get(name);
        if (port == null) {
            throw new IllegalArgumentException("no port mapping for " + name);
        }
        return port.intValue();
    }

    public CassandraCluster(
        @NotNull final Clock clock,
        @NotNull final String httpServerBaseUrl,
        @NotNull final ExecutorCounter execCounter,
        @NotNull final PersistedCassandraClusterState clusterState,
        @NotNull final PersistedCassandraClusterHealthCheckHistory healthCheckHistory,
        @NotNull final PersistedCassandraClusterJobs jobsState,
        @NotNull final PersistedCassandraFrameworkConfiguration configuration
    ) {
        this.clock = clock;
        this.httpServerBaseUrl = httpServerBaseUrl;
        this.execCounter = execCounter;
        this.clusterState = clusterState;
        this.healthCheckHistory = healthCheckHistory;
        this.jobsState = jobsState;
        this.configuration = configuration;

        clusterJobHandlers = new EnumMap<>(ClusterJobType.class);
        clusterJobHandlers.put(ClusterJobType.CLEANUP, new NodeTaskClusterJobHandler(this, jobsState));
        clusterJobHandlers.put(ClusterJobType.REPAIR, new NodeTaskClusterJobHandler(this, jobsState));
        clusterJobHandlers.put(ClusterJobType.RESTART, new RestartClusterJobHandler(this, jobsState));
    }

    @NotNull
    public PersistedCassandraClusterState getClusterState() {
        return clusterState;
    }

    @NotNull
    public PersistedCassandraFrameworkConfiguration getConfiguration() {
        return configuration;
    }

    public ExecutorMetadata metadataForExecutor(@NotNull String executorId) {
        for (ExecutorMetadata executorMetadata : clusterState.executorMetadata()) {
            if (executorId.equals(executorMetadata.getExecutorId())) {
                return executorMetadata;
            }
        }
        return null;
    }

    public void removeTask(@NotNull final String taskId, @NotNull Protos.TaskStatus status) {
        List<CassandraNode> nodes = clusterState.nodes();
        List<CassandraNode> newNodes = new ArrayList<>(nodes.size());
        boolean changed = false;
        for (CassandraNode cassandraNode : nodes) {
            CassandraNodeTask nodeTask = CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, taskId);
            if (nodeTask == null) {
                newNodes.add(cassandraNode);
                continue;
            }
            CassandraNode.Builder builder = CassandraFrameworkProtosUtils.removeTask(cassandraNode, nodeTask);
            changed = true;
            switch (nodeTask.getType()) {
                case METADATA:
                    // TODO shouldn't we also assume that the server task is no longer running ??
                    // TODO do we need to remove the executor metadata ??

                    removeExecutorMetadata(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    builder.clearTasks();
                    break;
                case SERVER:
                    builder.clearCassandraDaemonPid();
                    if (status.hasSource()) {
                        switch (status.getSource()) {
                            case SOURCE_MASTER:
                            case SOURCE_SLAVE:
                                recordHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId(),
                                    HealthCheckDetails.newBuilder()
                                        .setHealthy(false)
                                        .setMsg("Removing Cassandra server task " + taskId + ". Reason=" + status.getReason() + ", source=" + status.getSource() + ", message=\"" + status.getMessage() + '"')
                                        .build());
                                break;
                        }
                    }
                    break;
                case CLUSTER_JOB:
                    jobsState.clearClusterJobCurrentNode(status.getExecutorId().getValue());
                    break;
            }

            newNodes.add(builder.build());
        }
        if (changed) {
            clusterState.nodes(newNodes);
        }

        ClusterJobStatus clusterJob = getCurrentClusterJob();
        if (clusterJob != null) {
            if (clusterJob.hasCurrentNode() && clusterJob.getCurrentNode().getTaskId().equals(taskId)) {
                clusterJobHandlers.get(clusterJob.getJobType()).onTaskRemoved(status, clusterJob);
            }
        }
    }

    public void removeExecutor(@NotNull final String executorId) {
        final FluentIterable<CassandraNode> update = from(clusterState.nodes())
            .transform(cassandraNodeToBuilder())
            .transform(new ContinuingTransform<CassandraNode.Builder>() {
                @Override
                public CassandraNode.Builder apply(final CassandraNode.Builder input) {
                    if (input.hasCassandraNodeExecutor() && executorTaskId(input).equals(executorId)) {
                        return input
                            .clearTasks();
                    }
                    return input;
                }
            })
            .transform(cassandraNodeBuilderToCassandraNode());
        jobsState.clearClusterJobCurrentNode(executorId);
        clusterState.nodes(newArrayList(update));
        removeExecutorMetadata(executorId);
    }

    @NotNull
    public Optional<String> getExecutorIdForTask(@NotNull final String taskId) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeForTaskId(taskId))
                .filter(cassandraNodeHasExecutor())
                .transform(executorIdFromCassandraNode())
        );
    }

    @NotNull
    public Optional<CassandraNode> getNodeForTask(@NotNull final String taskId) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeForTaskId(taskId))
                .filter(cassandraNodeHasExecutor())
        );
    }

    public void updateNodeExecutors() {
        List<CassandraNode> nodes = new ArrayList<>();
        for (CassandraNode node : clusterState.nodes()) {
            nodes.add(CassandraNode.newBuilder(node)
                .setCassandraNodeExecutor(buildCassandraNodeExecutor(node.getCassandraNodeExecutor().getExecutorId()))
                .build());
        }
        clusterState.nodes(nodes);
    }

    public void addExecutorMetadata(@NotNull final ExecutorMetadata executorMetadata) {
        clusterState.executorMetadata(append(
            clusterState.executorMetadata(),
            executorMetadata
        ));
    }

    private void removeExecutorMetadata(@NotNull final String executorId) {
        final FluentIterable<ExecutorMetadata> update = from(clusterState.executorMetadata())
            .filter(not(new Predicate<ExecutorMetadata>() {
                @Override
                public boolean apply(final ExecutorMetadata input) {
                    return input.getExecutorId().equals(executorId);
                }
            }));
        clusterState.executorMetadata(newArrayList(update));
    }

    private boolean shouldRunHealthCheck(@NotNull final String executorID) {
        final Optional<Long> previousHealthCheckTime = headOption(
            from(healthCheckHistory.entries())
                .filter(healthCheckHistoryEntryExecutorIdEq(executorID))
                .transform(healthCheckHistoryEntryToTimestamp())
                .toSortedList(Collections.reverseOrder(naturalLongComparator))
        );

        if (configuration.healthCheckInterval().toDuration().getStandardSeconds() <= 0) {
            return false;
        }

        if (previousHealthCheckTime.isPresent()) {
            final Duration duration = new Duration(new Instant(previousHealthCheckTime.get()), clock.now());
            return duration.isLongerThan(configuration.healthCheckInterval());
        } else {
            return true;
        }
    }

    public HealthCheckHistoryEntry lastHealthCheck(@NotNull final String executorId) {
        return healthCheckHistory.last(executorId);
    }

    public void recordHealthCheck(@NotNull final String executorId, @NotNull final HealthCheckDetails details) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("> recordHealthCheck(executorId : {}, details : {})", executorId, protoToString(details));
        }
        Optional<CassandraNode> nodeOpt = cassandraNodeForExecutorId(executorId);
        if (nodeOpt.isPresent()) {
            if (!details.getHealthy()) {
                LOGGER.info(
                    "health check result unhealthy for node: {}. Message: '{}'",
                    nodeOpt.get().hasCassandraNodeExecutor() ? nodeOpt.get().getCassandraNodeExecutor().getExecutorId() : nodeOpt.get().getHostname(),
                    details.getMsg()
                );
            } else {
                // upon the first healthy response clear the replacementForIp field
                CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(nodeOpt.get(), CassandraNodeTask.NodeTaskType.SERVER);
                if (serverTask != null && nodeOpt.get().hasReplacementForIp()) {
                    clusterState.nodeReplaced(nodeOpt.get());
                }
            }
        }
        healthCheckHistory.record(executorId, clock.now().getMillis(), details);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("< recordHealthCheck(executorId : {}, details : {})", executorId, protoToString(details));
        }
    }

    @NotNull
    public List<String> getSeedNodeIps() {
        return CassandraFrameworkProtosUtils.getSeedNodeIps(clusterState.nodes());
    }

    @NotNull
    public List<CassandraNode> getLiveSeedNodes() {
        List<CassandraNode> nodes = new ArrayList<>();
        for (CassandraNode n : clusterState.nodes()) {
            if (n.getSeed() && isLiveNode(n)) {
                nodes.add(n);
            }
        }
        return nodes;
    }

    public TasksForOffer getTasksForOffer(@NotNull final Protos.Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "> getTasksForOffer(offer : {})", protoToString(offer));
        }

        try {
            final Optional<CassandraNode> nodeOption = cassandraNodeForHostname(offer.getHostname());

            CassandraNode.Builder node;
            if (!nodeOption.isPresent()) {
                if (clusterState.get().getNodesToAcquire() <= 0) {
                    // number of C* cluster nodes already present
                    return null;
                }

                String replacementForIp = clusterState.nextReplacementIp();

                boolean buildSeedNode = clusterState.doAcquireNewNodeAsSeed();
                CassandraNode newNode = buildCassandraNode(offer, buildSeedNode, replacementForIp);
                clusterState.nodeAcquired(newNode);
                node = CassandraNode.newBuilder(newNode);
            } else {
                node = CassandraNode.newBuilder(nodeOption.get());
            }

            if (!node.hasCassandraNodeExecutor()) {
                if (node.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
                    // node completely terminated
                    return null;
                }
                final String executorId = getExecutorIdForOffer(offer);
                final CassandraNodeExecutor executor = buildCassandraNodeExecutor(executorId);
                node.setCassandraNodeExecutor(executor);
            }

            TasksForOffer result = new TasksForOffer(node.getCassandraNodeExecutor());

            final CassandraNodeExecutor executor = node.getCassandraNodeExecutor();
            final String executorId = executor.getExecutorId();
            CassandraNodeTask metadataTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.NodeTaskType.METADATA);
            CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.NodeTaskType.SERVER);
            if (metadataTask == null) {
                if (node.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
                    // node completely terminated
                    return null;
                }
                metadataTask = getMetadataTask(executorId, node.getIp());
                node.addTasks(metadataTask);
                result.getLaunchTasks().add(metadataTask);
            } else {
                final Optional<ExecutorMetadata> maybeMetadata = getExecutorMetadata(executorId);
                if (maybeMetadata.isPresent()) {

                    handleClusterTask(executorId, result);

                    if (serverTask == null) {
                        switch (node.getTargetRunState()) {
                            case RUN:
                                break;
                            case TERMINATE:

                                LOGGER.info(marker, "Killing executor {}", executorTaskId(node));
                                result.getKillTasks().add(Protos.TaskID.newBuilder().setValue(executorTaskId(node)).build());

                                return result;
                            case STOP:
                                LOGGER.debug(marker, "Cannot launch server (targetRunState==STOP)");
                                return null;
                            case RESTART:
                                // change state to run (RESTART complete)
                                node.setTargetRunState(CassandraNode.TargetRunState.RUN);
                                break;
                        }

                        if (clusterState.get().getSeedsToAcquire() > 0) {
                            // we do not have enough executor metadata records to fulfil seed node requirement
                            LOGGER.debug(marker, "Cannot launch non-seed node (seed node requirement not fulfilled)");
                            return null;
                        }

                        if (!canLaunchServerTask()) {
                            LOGGER.debug(marker, "Cannot launch server (timed)");
                            return null;
                        }

                        if (!node.getSeed()) {
                            // when starting a non-seed node also check if at least one seed node is running
                            // (otherwise that node will fail to start)
                            boolean anySeedRunning = false;
                            boolean anyNodeInfluencingTopology = false;
                            for (CassandraNode cassandraNode : clusterState.nodes()) {
                                if (CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, CassandraNodeTask.NodeTaskType.SERVER) != null) {
                                    HealthCheckHistoryEntry lastHC = lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                                    if (cassandraNode.getSeed()) {
                                        if (lastHC != null && lastHC.getDetails() != null && lastHC.getDetails().getInfo() != null
                                            && lastHC.getDetails().getHealthy()
                                            && lastHC.getDetails().getInfo().getJoined() && "NORMAL".equals(lastHC.getDetails().getInfo().getOperationMode())) {
                                            anySeedRunning = true;
                                        }
                                    }
                                    if (lastHC != null && lastHC.getDetails() != null && lastHC.getDetails().getInfo() != null
                                        && lastHC.getDetails().getHealthy()
                                        && (!lastHC.getDetails().getInfo().getJoined() || !"NORMAL".equals(lastHC.getDetails().getInfo().getOperationMode()))) {
                                        if (LOGGER.isDebugEnabled()) {
                                            LOGGER.debug("Cannot start server task because of operation mode '{}' on node '{}'", lastHC.getDetails().getInfo().getOperationMode(), cassandraNode.getHostname());
                                        }
                                        anyNodeInfluencingTopology = true;
                                    }
                                }
                            }
                            if (!anySeedRunning) {
                                LOGGER.debug("Cannot start server task because no seed node is running");
                                return null;
                            }
                            if (anyNodeInfluencingTopology) {
                                return null;
                            }
                        }

                        CassandraFrameworkConfiguration config = configuration.get();
                        CassandraConfigRole configRole = configuration.getDefaultConfigRole();
                        final List<String> errors = hasResources(
                                offer,
                                configRole.getResources(),
                                portMappings(config),
                                configRole.getMesosRole()
                        );
                        if (!errors.isEmpty()) {
                            LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                        } else {
                            final ExecutorMetadata metadata = maybeMetadata.get();
                            final CassandraNodeTask task = getServerTask(serverTaskId(node), serverTaskName(), metadata, node);
                            node.addTasks(task)
                                .setNeedsConfigUpdate(false);
                            result.getLaunchTasks().add(task);

                            clusterState.updateLastServerLaunchTimestamp(clock.now().getMillis());
                        }
                    } else {
                        if (node.getNeedsConfigUpdate()) {
                            CassandraNodeTask task = getConfigUpdateTask(configUpdateTaskId(node), maybeMetadata.get());
                            node.addTasks(task)
                                .setNeedsConfigUpdate(false);
                            result.getLaunchTasks().add(task);
                        }

                        switch (node.getTargetRunState()) {
                            case RUN:
                                if (shouldRunHealthCheck(executorId)) {
                                    result.getSubmitTasks().add(getHealthCheckTaskDetails());
                                }

                                break;
                            case STOP:
                            case RESTART:
                            case TERMINATE:
                                // stop node for STOP and RESTART

                                LOGGER.info(marker, "Killing server task {}", serverTaskId(node));
                                result.getKillTasks().add(Protos.TaskID.newBuilder().setValue(serverTaskId(node)).build());

                                break;
                        }
                    }
                }
            }

            if (!result.hasAnyTask()) {
                // nothing to do
                return null;
            }

            final CassandraNode built = node.build();
            clusterState.addOrSetNode(built);

            return result;
        } finally {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "< getTasksForOffer(offer : {}) = {}, {}", protoToString(offer));
            }
        }
    }


    @NotNull
    private static String executorTaskId(@NotNull CassandraNode.Builder node) {
        return node.getCassandraNodeExecutor().getExecutorId();
    }

    @NotNull
    private static String configUpdateTaskId(CassandraNode.Builder node) {
        return executorTaskId(node) + ".config";
    }

    @NotNull
    private String serverTaskName() {
        return configuration.frameworkName() + ".node";
    }

    @NotNull
    private static String serverTaskId(CassandraNode.Builder node) {
        return executorTaskId(node) + ".server";
    }

    private boolean canLaunchServerTask() {
        return clock.now().getMillis() >= nextPossibleServerLaunchTimestamp();
    }

    public long nextPossibleServerLaunchTimestamp() {
        long lastServerLaunchTimestamp = getClusterState().get().getLastServerLaunchTimestamp();
        long seconds = Math.max(getConfiguration().get().getBootstrapGraceTimeSeconds(), getConfiguration().get().getHealthCheckIntervalSeconds());
        return lastServerLaunchTimestamp + seconds * 1000L;
    }

    @NotNull
    public Optional<CassandraNode> cassandraNodeForHostname(String hostname) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeHostnameEq(hostname))
        );
    }

    @NotNull
    public Optional<CassandraNode> cassandraNodeForExecutorId(String executorId) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeExecutorIdEq(executorId))
        );
    }

    private CassandraNode buildCassandraNode(Protos.Offer offer, boolean seed, String replacementForIp) {
        CassandraNode.Builder builder = CassandraNode.newBuilder()
                .setHostname(offer.getHostname())
                .setTargetRunState(CassandraNode.TargetRunState.RUN)
                .addDataVolumes(
                    DataVolume.newBuilder()
                        .setPath(configuration.getDefaultConfigRole().getPreDefinedDataDirectory())
                )
                .setSeed(seed);
        if (replacementForIp != null) {
            builder.setReplacementForIp(replacementForIp);
        }

        try {
            InetAddress ia = InetAddress.getByName(offer.getHostname());

            int jmxPort = getPortMapping(PORT_JMX);
            if (ia.isLoopbackAddress()) {
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    jmxPort = serverSocket.getLocalPort();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return builder.setIp(ia.getHostAddress())
                    .setJmxConnect(JmxConnect.newBuilder()
                                    .setIp("127.0.0.1")
                                    .setJmxPort(jmxPort)
                            // TODO JMX auth parameters go here
                    )
                    .build();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private int getPortMapping(String name) {
        return getPortMapping(configuration.get(), name);
    }

    private static Map<String, Long> portMappings(CassandraFrameworkConfiguration config) {
        Map<String, Long> r = new HashMap<>();
        for (String name : defaultPortMappings.keySet()) {
            r.put(name, (long) getPortMapping(config, name));
        }
        return r;
    }

    @NotNull
    private String getExecutorIdForOffer(@NotNull final Protos.Offer offer) {
        final FluentIterable<CassandraNode> filter =
            from(clusterState.nodes())
                .filter(cassandraNodeHasExecutor())
                .filter(cassandraNodeHostnameEq(offer.getHostname()));
        if (filter.isEmpty()) {
            return configuration.frameworkName() + ".node." + execCounter.getAndIncrement() + ".executor";
        } else {
            return filter.get(0).getCassandraNodeExecutor().getExecutorId();
        }
    }

    @NotNull
    private String getUrlForResource(@NotNull final String resourceName) {
        return URL_FOR_RESOURCE_REPLACE.matcher((httpServerBaseUrl + '/' + resourceName)).replaceAll("/");
    }

    @NotNull
    private CassandraNodeTask getConfigUpdateTask(
        @NotNull final String taskId,
        @NotNull final ExecutorMetadata metadata) {
        CassandraFrameworkConfiguration config = configuration.get();
        CassandraConfigRole configRole = config.getDefaultConfigRole();

        CassandraServerConfig cassandraServerConfig = buildCassandraServerConfig(metadata, config, configRole, TaskEnv.getDefaultInstance());

        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setType(TaskDetails.TaskDetailsType.UPDATE_CONFIG)
            .setUpdateConfigTask(UpdateConfigTask.newBuilder()
                    .setCassandraServerConfig(cassandraServerConfig)
            )
            .build();

        return CassandraNodeTask.newBuilder()
            .setType(CassandraNodeTask.NodeTaskType.CONFIG)
            .setTaskId(taskId)
            .setResources(configRole.getResources())
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    private CassandraNodeTask getServerTask(
        @NotNull final String taskId,
        @NotNull final String taskName,
        @NotNull final ExecutorMetadata metadata,
        @NotNull final CassandraNode.Builder node) {
        CassandraFrameworkConfiguration config = configuration.get();
        CassandraConfigRole configRole = config.getDefaultConfigRole();

        final TaskEnv.Builder taskEnv = TaskEnv.newBuilder();
        for (TaskEnv.Entry entry : configRole.getTaskEnv().getVariablesList()) {
            if (!"JMX_PORT".equals(entry.getName()) && !"MAX_HEAP_SIZE".equals(entry.getName())) {
                taskEnv.addVariables(entry);
            }
        }
        // see conf/cassandra-env.sh in the cassandra distribution for details
        // about these variables.
        CassandraFrameworkProtosUtils.addTaskEnvEntry(taskEnv, true, "JMX_PORT", String.valueOf(node.getJmxConnect().getJmxPort()));
        CassandraFrameworkProtosUtils.addTaskEnvEntry(taskEnv, true, "MAX_HEAP_SIZE", configRole.getMemJavaHeapMb() + "m");
        // The example HEAP_NEWSIZE assumes a modern 8-core+ machine for decent pause
        // times. If in doubt, and if you do not particularly want to tweak, go with
        // 100 MB per physical CPU core.
        CassandraFrameworkProtosUtils.addTaskEnvEntry(taskEnv, false, "HEAP_NEWSIZE", (int) (configRole.getResources().getCpuCores() * 100) + "m");

        ArrayList<String> command = newArrayList("apache-cassandra-" + configRole.getCassandraVersion() + "/bin/cassandra", "-f");
        if (node.hasReplacementForIp()) {
            command.add("-Dcassandra.replace_address=" + node.getReplacementForIp());
        }

        CassandraServerConfig cassandraServerConfig = buildCassandraServerConfig(metadata, config, configRole, taskEnv.build());

        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setType(TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN)
            .setCassandraServerRunTask(
                CassandraServerRunTask.newBuilder()
                    // have to start it in foreground in order to be able to detect runtime status in the executor
                    .addAllCommand(command)
                    .setVersion(configRole.getCassandraVersion())
                    .setCassandraServerConfig(cassandraServerConfig)
                    .setVersion(configRole.getCassandraVersion())
                    .setJmx(node.getJmxConnect())
            )
            .build();

        return CassandraNodeTask.newBuilder()
            .setType(CassandraNodeTask.NodeTaskType.SERVER)
            .setTaskId(taskId)
            .setTaskName(taskName)
            .setResources(TaskResources.newBuilder(configRole.getResources())
                .addAllPorts(portMappings(config).values()))
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    private CassandraServerConfig buildCassandraServerConfig(
        @NotNull ExecutorMetadata metadata,
            @NotNull CassandraFrameworkConfiguration config,
            @NotNull CassandraConfigRole configRole,
            @NotNull TaskEnv taskEnv) {
        final TaskConfig.Builder taskConfig = TaskConfig.newBuilder(configRole.getCassandraYamlConfig());
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("cluster_name", config.getFrameworkName()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("broadcast_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("rpc_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("listen_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("storage_port", getPortMapping(config, PORT_STORAGE)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("ssl_storage_port", getPortMapping(config, PORT_STORAGE_SSL)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("native_transport_port", getPortMapping(config, PORT_NATIVE)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("rpc_port", getPortMapping(config, PORT_RPC)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("seeds", SEEDS_FORMAT_JOINER.join(getSeedNodeIps())));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("endpoint_snitch", config.hasSnitch() ? config.getSnitch() : "GossipingPropertyFileSnitch"));
        // data directory config
        // TODO: Update the logic here for defining data directories when mesos persistent volumes are released
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("data_file_directories", newArrayList(configRole.getPreDefinedDataDirectory() + "/data")));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("commitlog_directory", configRole.getPreDefinedDataDirectory() + "/commitlog"));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("saved_caches_directory", configRole.getPreDefinedDataDirectory() + "/saved_caches"));

        return CassandraServerConfig.newBuilder()
            .setCassandraYamlConfig(taskConfig)
            .setTaskEnv(taskEnv)
            .build();
    }

    @NotNull
    private Optional<ExecutorMetadata> getExecutorMetadata(@NotNull final String executorId) {
        final FluentIterable<ExecutorMetadata> filter = from(clusterState.executorMetadata())
            .filter(executorMetadataExecutorIdEq(executorId));
        return headOption(filter);
    }

    @NotNull
    private CassandraNodeExecutor buildCassandraNodeExecutor(@NotNull final String executorId) {
        String osName = Env.option("OS_NAME").or(Env.osFromSystemProperty());
        String javaExec = "macosx".equals(osName)
            ? "$(pwd)/jre*/Contents/Home/bin/java"
            : "$(pwd)/jre*/bin/java";

        CassandraConfigRole configRole = configuration.getDefaultConfigRole();

        List<String> command = newArrayList(
            javaExec,
//            "agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
            "-XX:+PrintCommandLineFlags",
            "$JAVA_OPTS",
            "-classpath",
            "cassandra-executor.jar",
            "io.mesosphere.mesos.frameworks.cassandra.executor.CassandraExecutor");

        return CassandraNodeExecutor.newBuilder()
            .setExecutorId(executorId)
            .setSource(configuration.frameworkName())
            .addAllCommand(command)
            .setTaskEnv(taskEnvFromMap(executorEnv))
            .setResources(taskResources(0.1, 384, 256))
            .addAllDownload(newArrayList(
                resourceFileDownload(getUrlForResource("/jre-7-" + osName + ".tar.gz"), true),
                resourceFileDownload(getUrlForResource("/apache-cassandra-" + configRole.getCassandraVersion() + "-bin.tar.gz"), true),
                resourceFileDownload(getUrlForResource("/cassandra-executor.jar"), false)
            ))
            .build();
    }

    private static TaskDetails getHealthCheckTaskDetails() {
        return TaskDetails.newBuilder()
            .setType(TaskDetails.TaskDetailsType.HEALTH_CHECK)
            .build();
    }

    @NotNull
    private static CassandraNodeTask getMetadataTask(@NotNull final String executorId, String ip) {
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setType(TaskDetails.TaskDetailsType.EXECUTOR_METADATA)
            .setExecutorMetadataTask(
                ExecutorMetadataTask.newBuilder()
                    .setExecutorId(executorId)
                    .setIp(ip)
            )
            .build();
        return CassandraNodeTask.newBuilder()
            .setType(CassandraNodeTask.NodeTaskType.METADATA)
            .setTaskId(executorId)
            .setResources(taskResources(0.1, 32, 0))
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    static List<String> hasResources(
        @NotNull final Protos.Offer offer,
        @NotNull final TaskResources resources,
        @NotNull final Map<String, Long> portMapping,
        final String mesosRole
    ) {
        final List<String> errors = newArrayList();
        final ListMultimap<String, Protos.Resource> index = from(offer.getResourcesList())
                .filter(resourceHasExpectedRole(mesosRole))
                .index(resourceToName());


        final double availableCpus = resourceValueDouble(headOption(index.get("cpus"))).or(0.0);
        final long availableMem = resourceValueLong(headOption(index.get("mem"))).or(0L);
        final long availableDisk = resourceValueLong(headOption(index.get("disk"))).or(0L);

        if (availableCpus < resources.getCpuCores()) {
            errors.add(String.format("Not enough cpu resources for role %s. Required %s only %s available", mesosRole, String.valueOf(resources.getCpuCores()), String.valueOf(availableCpus)));
        }
        if (availableMem < resources.getMemMb()) {
            errors.add(String.format("Not enough mem resources for role %s. Required %d only %d available", mesosRole, resources.getMemMb(), availableMem));
        }
        if (availableDisk < resources.getDiskMb()) {
            errors.add(String.format("Not enough disk resources for role %s. Required %d only %d available", mesosRole, resources.getDiskMb(), availableDisk));
        }

        final TreeSet<Long> ports = resourceValueRange(headOption(index.get("ports")));
        for (final Map.Entry<String, Long> entry : portMapping.entrySet()) {
            final String key = entry.getKey();
            final Long value = entry.getValue();
            if (!ports.contains(value)) {
                errors.add(String.format("Unavailable port %d(%s) for role %s. %d other ports available", value, key, mesosRole, ports.size()));
            }
        }
        return errors;
    }

    public int updateNodeCount(int nodeCount) {
        int currentNodeCount = clusterState.nodeCounts().getNodeCount() + clusterState.get().getNodesToAcquire();
        int newNodeCount = nodeCount - currentNodeCount;
        if (newNodeCount < 0) {
            LOGGER.info("Cannot shrink number of nodes from {} to {}", currentNodeCount, nodeCount);
            return currentNodeCount;
        } else if (newNodeCount > 0) {
            clusterState.acquireNewNodes(newNodeCount);
        }
        return nodeCount;
    }

    // cluster tasks

    private void handleClusterTask(String executorId, TasksForOffer tasksForOffer) {
        ClusterJobStatus currentJob = getCurrentClusterJob();
        if (currentJob == null) {
            return;
        }

        Optional<CassandraNode> nodeForExecutorId = cassandraNodeForExecutorId(executorId);

        clusterJobHandlers.get(currentJob.getJobType()).handleTaskOffer(currentJob, executorId, nodeForExecutorId, tasksForOffer);
    }

    public void onNodeJobStatus(SlaveStatusDetails statusDetails) {
        ClusterJobStatus currentJob = getCurrentClusterJob();
        if (currentJob == null) {
            return;
        }

        if (!statusDetails.hasNodeJobStatus()) {
            // TODO add some failure handling here
            return;
        }

        NodeJobStatus nodeJobStatus = statusDetails.getNodeJobStatus();

        if (currentJob.getJobType() != nodeJobStatus.getJobType()) {
            // oops - status message of other type...  ignore for now
            LOGGER.warn("Got node job status of tye {} - but expected {}", nodeJobStatus.getJobType(), currentJob.getJobType());
            return;
        }

        clusterJobHandlers.get(currentJob.getJobType()).onNodeJobStatus(currentJob, nodeJobStatus);
    }

    public boolean startClusterTask(ClusterJobType jobType) {
        if (jobsState.get().hasCurrentClusterJob()) {
            return false;
        }

        ClusterJobStatus.Builder builder = ClusterJobStatus.newBuilder()
                .setJobType(jobType)
                .setStartedTimestamp(clock.now().getMillis());

        for (CassandraNode cassandraNode : clusterState.nodes()) {
            if (cassandraNode.hasCassandraNodeExecutor()) {
                builder.addRemainingNodes(cassandraNode.getCassandraNodeExecutor().getExecutorId());
            }
        }

        jobsState.setCurrentJob(builder.build());

        return true;
    }

    public boolean abortClusterJob(ClusterJobType jobType) {
        ClusterJobStatus current = getCurrentClusterJob(jobType);
        if (current == null || current.getAborted()) {
            return false;
        }

        current = ClusterJobStatus.newBuilder(current)
                .setAborted(true).build();
        jobsState.setCurrentJob(current);
        return true;
    }

    public ClusterJobStatus getCurrentClusterJob() {
        CassandraClusterJobs jobState = jobsState.get();
        return jobState.hasCurrentClusterJob() ? jobState.getCurrentClusterJob() : null;
    }

    public ClusterJobStatus getCurrentClusterJob(ClusterJobType jobType) {
        ClusterJobStatus current = getCurrentClusterJob();
        return current != null && current.getJobType() == jobType ? current : null;
    }

    public ClusterJobStatus getLastClusterJob(ClusterJobType jobType) {
        List<ClusterJobStatus> list = jobsState.get().getLastClusterJobsList();
        if (list == null) {
            return null;
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            ClusterJobStatus clusterJobStatus = list.get(i);
            if (clusterJobStatus.getJobType() == jobType) {
                return clusterJobStatus;
            }
        }
        return null;
    }

    public CassandraNode findNode(String node) {
        for (CassandraNode cassandraNode : clusterState.nodes()) {
            if (cassandraNode.getIp().equals(node)
                || cassandraNode.getHostname().equals(node)
                || (cassandraNode.hasCassandraNodeExecutor() && cassandraNode.getCassandraNodeExecutor().getExecutorId().equals(node))) {
                return cassandraNode;
            }
        }
        return null;
    }

    public CassandraNode nodeStop(String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.STOP);
    }

    public CassandraNode nodeRun(String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.RUN);
    }

    public CassandraNode nodeRestart(String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.RESTART);
    }

    public CassandraNode nodeTerminate(String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.TERMINATE);
    }

    public CassandraNode updateNodeTargetRunState(String node, CassandraNode.TargetRunState targetRunState) {
        CassandraNode cassandraNode = findNode(node);
        return updateNodeTargetRunState(cassandraNode, targetRunState);
    }

    public CassandraNode updateNodeTargetRunState(CassandraNode cassandraNode, CassandraNode.TargetRunState targetRunState) {
        if (cassandraNode == null ||
            cassandraNode.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE ||
            cassandraNode.getTargetRunState() == targetRunState) {
            return cassandraNode;
        }

        cassandraNode = CassandraNode.newBuilder(cassandraNode)
            .setTargetRunState(targetRunState)
            .build();
        clusterState.addOrSetNode(cassandraNode);

        return cassandraNode;
    }

    public void updateCassandraProcess(@NotNull Protos.ExecutorID executorId, @NotNull CassandraServerRunMetadata cassandraServerRunMetadata) {
        Optional<CassandraNode> node = cassandraNodeForExecutorId(executorId.getValue());
        if (node.isPresent()) {
            clusterState.addOrSetNode(CassandraFrameworkProtos.CassandraNode.newBuilder(node.get())
                .setCassandraDaemonPid(cassandraServerRunMetadata.getPid())
                .build());
        }
    }

    public List<CassandraNode> liveNodes(int limit) {
        CassandraClusterState state = clusterState.get();
        int total = state.getNodesCount();
        if (total == 0) {
            return Collections.emptyList();
        }

        int totalLive = 0;
        for (int i = 0; i < total; i++) {
            if (isLiveNode(state.getNodes(i))) {
                totalLive++;
            }
        }

        limit = Math.min(totalLive, limit);

        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        List<CassandraNode> result = new ArrayList<>(limit);
        int misses = 0;
        while (result.size() < limit && misses < 250) { // the check for 250 misses is a poor-man's implementation to prevent a possible race-condition
            int i = tlr.nextInt(total);
            CassandraNode node = state.getNodes(i);
            if (isLiveNode(node) && !result.contains(node)) {
                result.add(node);
                misses = 0;
            } else {
                misses++;
            }
        }

        return result;
    }

    public boolean isLiveNode(CassandraNode node) {
        if (!node.hasCassandraNodeExecutor()) {
            return false;
        }
        if (getTaskForNode(node, CassandraNodeTask.NodeTaskType.SERVER) == null) {
            return false;
        }
        HealthCheckHistoryEntry hc = lastHealthCheck(node.getCassandraNodeExecutor().getExecutorId());
        return isLiveNode(hc);
    }

    public boolean isLiveNode(HealthCheckHistoryEntry hc) {
        if (hc == null || !hc.hasDetails()) {
            return false;
        }
        HealthCheckDetails hcd = hc.getDetails();
        if (!hcd.getHealthy() || !hcd.hasInfo()) {
            return false;
        }
        NodeInfo info = hcd.getInfo();
        if (!info.hasNativeTransportRunning() || !info.hasRpcServerRunning()) {
            return false;
        }
        return info.getNativeTransportRunning() && info.getRpcServerRunning();
    }

    public CassandraNode replaceNode(String node) throws ReplaceNodePreconditionFailed {
        CassandraNode cassandraNode = findNode(node);
        if (cassandraNode == null) {
            throw new ReplaceNodePreconditionFailed("Non-existing node " + node);
        }

        if (cassandraNode.getSeed()) {
            throw new ReplaceNodePreconditionFailed("Node " + node + " to replace is a seed node");
        }

        if (isLiveNode(cassandraNode)) {
            throw new ReplaceNodePreconditionFailed("Cannot replace live node " + node + " - terminate it first");
        }

        if (cassandraNode.getTargetRunState() != CassandraNode.TargetRunState.TERMINATE) {
            throw new ReplaceNodePreconditionFailed("Cannot replace non-terminated node " + node + " - terminate it first");
        }

        if (!cassandraNode.getTasksList().isEmpty()) {
            throw new ReplaceNodePreconditionFailed("Node " + node + " to replace has active tasks");
        }

        if (clusterState.get().getReplaceNodeIpsList().contains(cassandraNode.getIp())) {
            throw new ReplaceNodePreconditionFailed("Node " + node + " already in replace-list");
        }

        clusterState.replaceNode(cassandraNode.getIp());
        return cassandraNode;
    }

    @NotNull
    public List<String> getNodeLogFiles(@NotNull CassandraNode cassandraNode) {

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata = metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
        if (executorMetadata == null) {
            return Collections.emptyList();
        }

        String workdir = executorMetadata.getWorkdir();
        return newArrayList(
            workdir + "/executor.log",
            workdir + "/apache-cassandra-" + getConfiguration().getDefaultConfigRole().getCassandraVersion() + "/logs/system.log");
    }

    public boolean setNodeSeed(CassandraNode cassandraNode, boolean seed) throws SeedChangeException {
        if (cassandraNode.getSeed() == seed) {
            return false;
        }

        List<CassandraNode> liveSeedNodes = getLiveSeedNodes();

        if (clusterState.get().getSeedsToAcquire() > 0) {
            throw new SeedChangeException("Must not change seed status while initial number of seed nodes has not been acquired");
        }

        if (liveSeedNodes.size() == 1 && liveSeedNodes.get(0).getHostname().equals(cassandraNode.getHostname())) {
            // node to change is the last live seed node - abort
            throw new SeedChangeException("Must not remove the last live seed node");
        }

        clusterState.setNodeAndUpdateConfig(CassandraNode.newBuilder(cassandraNode)
            .setSeed(seed));

        return true;
    }

    private static TaskResources taskResources(double cpuCores, long memMb, long diskMb) {
        return TaskResources.newBuilder()
            .setCpuCores(cpuCores)
            .setMemMb(memMb)
            .setDiskMb(diskMb)
            .build();
    }
}
