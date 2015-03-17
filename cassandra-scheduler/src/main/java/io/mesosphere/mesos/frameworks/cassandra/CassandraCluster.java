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
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
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

    public static int getPortMapping(CassandraFrameworkConfiguration configuration, String name) {
        for (PortMapping portMapping : configuration.getPortMappingList()) {
            if (portMapping.getName().equals(name))
                return portMapping.getPort();
        }
        Long port = defaultPortMappings.get(name);
        if (port == null) {
            throw new IllegalStateException("no port mapping for " + name);
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
    }

    @NotNull
    public PersistedCassandraClusterState getClusterState() {
        return clusterState;
    }

    @NotNull
    public PersistedCassandraFrameworkConfiguration getConfiguration() {
        return configuration;
    }

    @NotNull
    public PersistedCassandraClusterJobs getJobsState() {
        return jobsState;
    }

    public void removeTask(@NotNull final String taskId, Protos.TaskStatus status) {
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
            switch (nodeTask.getTaskType()) {
                case METADATA:
                    // TODO shouldn't we also assume that the server task is no longer running ??
                    // TODO do we need to remove the executor metadata ??

                    removeExecutorMetadata(nodeTask.getExecutorId());
                    builder.clearTasks();
                    break;
                case SERVER:
                    builder.clearCassandraDaemonPid();
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
                jobsState.removeTaskForCurrentNode(status, clusterJob);
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

    public void addExecutorMetadata(@NotNull final ExecutorMetadata executorMetadata) {
        clusterState.executorMetadata(append(
            clusterState.executorMetadata(),
            executorMetadata
        ));
    }

    void removeExecutorMetadata(@NotNull final String executorId) {
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
                // TODO: This needs to be smarter, right not it assumes that as soon as it's unhealth it's dead
                //removeTask(nodeOpt.get().getServerTask().getTaskId());
            } else {
                // upon the first healthy response clear the replacementForIp field
                CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(nodeOpt.get(), CassandraNodeTask.TaskType.SERVER);
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
    public List<String> getSeedNodes() {
        return CassandraFrameworkProtosUtils.getSeedNodeIps(clusterState.nodes());
    }

    public TasksForOffer getTasksForOffer(@NotNull final Protos.Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "> getTasksForOffer(offer : {})", protoToString(offer));
        }

        try {
            final Optional<CassandraNode> nodeOption = cassandraNodeForHostname(offer.getHostname());

            CassandraConfigRole defaultConfigRole = configuration.getDefaultConfigRole();

            CassandraNode.Builder node;
            if (!nodeOption.isPresent()) {
                if (clusterState.get().getNodesToAcquire() <= 0) {
                    // number of C* cluster nodes already present
                    return null;
                }

                NodeCounts nodeCounts = clusterState.nodeCounts();
                if (nodeCounts.getNodeCount() >= defaultConfigRole.getNumberOfNodes()) {
                    // number of C* cluster nodes already present
                    return null;
                }

                String replacementForIp = clusterState.nextReplacementIp();

                boolean buildSeedNode = nodeCounts.getSeedCount() < defaultConfigRole.getNumberOfSeeds();
                CassandraNode newNode = buildCassandraNode(offer, buildSeedNode, replacementForIp);
                clusterState.nodeAcquired(newNode);
                node = CassandraNode.newBuilder(newNode);
            } else
                node = CassandraNode.newBuilder(nodeOption.get());

            if (!node.hasCassandraNodeExecutor()) {
                if (node.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
                    // node completely terminated
                    return null;
                }
                final String executorId = getExecutorIdForOffer(offer);
                final CassandraNodeExecutor executor = getCassandraNodeExecutorSupplier(executorId);
                node.setCassandraNodeExecutor(executor);
            }

            TasksForOffer result = new TasksForOffer(node.getCassandraNodeExecutor());

            final CassandraNodeExecutor executor = node.getCassandraNodeExecutor();
            final String executorId = executor.getExecutorId();
            CassandraNodeTask metadataTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.TaskType.METADATA);
            CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.TaskType.SERVER);
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

                        if (clusterState.nodeCounts().getSeedCount() < defaultConfigRole.getNumberOfSeeds()) {
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
                                if (CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, CassandraNodeTask.TaskType.SERVER) != null) {
                                    HealthCheckHistoryEntry lastHC = lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                                    if (cassandraNode.getSeed()) {
                                        if (lastHC != null && lastHC.getDetails() != null && lastHC.getDetails().getInfo() != null
                                            && lastHC.getDetails().getHealthy()
                                            && lastHC.getDetails().getInfo().getJoined() && "NORMAL".equals(lastHC.getDetails().getInfo().getOperationMode()))
                                            anySeedRunning = true;
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
                                configRole.getCpuCores(),
                                configRole.getMemMb(),
                                configRole.getDiskMb(),
                                portMappings(config)
                        );
                        if (!errors.isEmpty()) {
                            LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                        } else {
                            final ExecutorMetadata metadata = maybeMetadata.get();
                            final CassandraNodeTask task = getServerTask(executorId, serverTaskId(node), metadata, node);
                            node.addTasks(task);
                            result.getLaunchTasks().add(task);

                            clusterState.updateLastServerLaunchTimestamp(clock.now().getMillis());
                        }
                    } else {
                        switch (node.getTargetRunState()) {
                            case RUN:
                                if (shouldRunHealthCheck(executorId)) {
                                    result.getSubmitTasks().add(getHealthCheckTaskDetails());
                                }

                                handleClusterTask(executorId, result);

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
    private String executorTaskId(@NotNull CassandraNode.Builder node) {
        return node.getCassandraNodeExecutor().getExecutorId();
    }

    @NotNull
    private String serverTaskId(@NotNull CassandraNode.Builder node) {
        return executorTaskId(node) + ".server";
    }

    private boolean canLaunchServerTask() {
        return clock.now().getMillis() > nextPossibleServerLaunchTimestamp();
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
    private CassandraNodeTask getServerTask(
        @NotNull final String executorId,
        @NotNull final String taskId,
        @NotNull final ExecutorMetadata metadata,
        @NotNull final CassandraNode.Builder node) {
        CassandraFrameworkConfiguration config = configuration.get();
        CassandraConfigRole configRole = config.getDefaultConfigRole();
        final TaskConfig.Builder taskConfig = TaskConfig.newBuilder(configRole.getCassandraYamlConfig());
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("cluster_name", config.getFrameworkName()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("broadcast_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("rpc_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("listen_address", metadata.getIp()));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("storage_port", getPortMapping(config, PORT_STORAGE)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("ssl_storage_port", getPortMapping(config, PORT_STORAGE_SSL)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("native_transport_port", getPortMapping(config, PORT_NATIVE)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("rpc_port", getPortMapping(config, PORT_RPC)));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("seeds", SEEDS_FORMAT_JOINER.join(getSeedNodes())));
        CassandraFrameworkProtosUtils.setTaskConfig(taskConfig, configValue("endpoint_snitch", config.hasSnitch() ? config.getSnitch() : "GossipingPropertyFileSnitch"));
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
        CassandraFrameworkProtosUtils.addTaskEnvEntry(taskEnv, false, "HEAP_NEWSIZE", (int) (configRole.getCpuCores() * 100) + "m");

        ArrayList<String> command = newArrayList("apache-cassandra-" + configRole.getCassandraVersion() + "/bin/cassandra", "-f");
        if (node.hasReplacementForIp()) {
            command.add("-Dcassandra.replace_address=" + node.getReplacementForIp());
        }
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.CASSANDRA_SERVER_RUN)
            .setCassandraServerRunTask(
                CassandraServerRunTask.newBuilder()
                    // have to start it in foreground in order to be able to detect runtime status in the executor
                    .addAllCommand(command)
                    .setTaskConfig(taskConfig)
                    .setVersion(configRole.getCassandraVersion())
                    .setTaskEnv(taskEnv)
                    .setJmx(node.getJmxConnect())
            )
            .build();

        return CassandraNodeTask.newBuilder()
            .setTaskType(CassandraNodeTask.TaskType.SERVER)
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .setCpuCores(configRole.getCpuCores())
            .setMemMb(configRole.getMemMb())
            .setDiskMb(configRole.getDiskMb())
            .addAllPorts(portMappings(config).values())
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    private Optional<ExecutorMetadata> getExecutorMetadata(@NotNull final String executorId) {
        final FluentIterable<ExecutorMetadata> filter = from(clusterState.executorMetadata())
            .filter(executorMetadataExecutorIdEq(executorId));
        return headOption(filter);
    }

    @NotNull
    private CassandraNodeExecutor getCassandraNodeExecutorSupplier(@NotNull final String executorId) {
        String osName = Env.option("OS_NAME").or(Env.osFromSystemProperty());
        String javaExec = "macosx".equals(osName)
            ? "$(pwd)/jre*/Contents/Home/bin/java"
            : "$(pwd)/jre*/bin/java";

        CassandraConfigRole configRole = configuration.getDefaultConfigRole();

        return CassandraNodeExecutor.newBuilder()
            .setExecutorId(executorId)
            .setSource(configuration.frameworkName())
            .setCpuCores(0.1)
            .setMemMb(16)
            .setDiskMb(16)
            .setCommand(javaExec)
//            .addCommandArgs("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
            .addCommandArgs("-XX:+PrintCommandLineFlags")
            .addCommandArgs("$JAVA_OPTS")
            .addCommandArgs("-classpath")
            .addCommandArgs("cassandra-executor.jar")
            .addCommandArgs("io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor")
            .setTaskEnv(taskEnvFromMap(executorEnv))
            .addAllResource(newArrayList(
                resourceUri(getUrlForResource("/jre-7-" + osName + ".tar.gz"), true),
                resourceUri(getUrlForResource("/apache-cassandra-" + configRole.getCassandraVersion() + "-bin.tar.gz"), true),
                resourceUri(getUrlForResource("/cassandra-executor.jar"), false)
            ))
            .build();
    }

    private static TaskDetails getHealthCheckTaskDetails() {
        return TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.HEALTH_CHECK)
            .build();
    }

    @NotNull
    private CassandraNodeTask getMetadataTask(@NotNull final String executorId, String ip) {
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.EXECUTOR_METADATA)
            .setExecutorMetadataTask(
                ExecutorMetadataTask.newBuilder()
                    .setExecutorId(executorId)
                    .setIp(ip)
            )
            .build();
        return CassandraNodeTask.newBuilder()
            .setTaskType(CassandraNodeTask.TaskType.METADATA)
            .setTaskId(executorId)
            .setExecutorId(executorId)
            .setCpuCores(0.1)
            .setMemMb(16)
            .setDiskMb(16)
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    private static List<String> hasResources(
        @NotNull final Protos.Offer offer,
        final double cpu,
        final long mem,
        final long disk,
        @NotNull final Map<String, Long> portMapping
    ) {
        final List<String> errors = newArrayList();
        final ListMultimap<String, Protos.Resource> index = from(offer.getResourcesList()).index(resourceToName());
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

    public int updateNodeCount(int nodeCount) {
        try {
            int newNodeCount = configuration.numberOfNodes(nodeCount);
            clusterState.acquireNewNodes(newNodeCount);
        } catch (IllegalArgumentException e) {
            LOGGER.info("Cannout update number-of-nodes", e);
        }
        return configuration.getDefaultConfigRole().getNumberOfNodes();
    }

    // cluster tasks

    private void handleClusterTask(String executorId, TasksForOffer tasksForOffer) {
        ClusterJobStatus currentTask = getCurrentClusterJob();
        if (currentTask == null)
            return;

        if (currentTask.hasCurrentNode()) {
            NodeJobStatus node = currentTask.getCurrentNode();
            if (executorId.equals(node.getExecutorId())) {
                // submit status request
                tasksForOffer.getSubmitTasks().add(TaskDetails.newBuilder()
                    .setTaskType(TaskDetails.TaskType.NODE_JOB_STATUS)
                    .build());

                LOGGER.info("Inquiring cluster job status for {} from {}", currentTask.getJobType().name(),
                        node.getExecutorId());

                return;
            }
            return;
        }

        if (currentTask.getAborted() && !currentTask.hasCurrentNode()) {
            jobsState.currentJob(null);
            // TODO record aborted job in history??
            return;
        }

        if (!currentTask.hasCurrentNode()) {
            List<String> remainingNodes = new ArrayList<>(currentTask.getRemainingNodesList());
            if (remainingNodes.isEmpty()) {
                jobsState.finishJob(currentTask);
                return;
            }

            if (!remainingNodes.remove(executorId)) {
                return;
            }

            Optional<CassandraNode> nextNode = cassandraNodeForExecutorId(executorId);
            if (!nextNode.isPresent()) {
                currentTask = ClusterJobStatus.newBuilder()
                        .clearRemainingNodes()
                        .addAllRemainingNodes(remainingNodes)
                        .build();
                jobsState.currentJob(currentTask);
                return;
            }


            final TaskDetails taskDetails = TaskDetails.newBuilder()
                    .setTaskType(TaskDetails.TaskType.NODE_JOB)
                    .setNodeJobTask(NodeJobTask.newBuilder().setJobType(currentTask.getJobType()))
                    .build();
            CassandraNodeTask cassandraNodeTask = CassandraNodeTask.newBuilder()
                    .setTaskType(CassandraNodeTask.TaskType.CLUSTER_JOB)
                    .setTaskId(executorId + '.' + currentTask.getJobType().name())
                    .setExecutorId(executorId)
                    .setCpuCores(0.1)
                    .setMemMb(16)
                    .setDiskMb(16)
                    .setTaskDetails(taskDetails)
                    .build();
            tasksForOffer.getLaunchTasks().add(cassandraNodeTask);

            NodeJobStatus currentNode = NodeJobStatus.newBuilder()
                    .setExecutorId(cassandraNodeTask.getExecutorId())
                    .setTaskId(cassandraNodeTask.getTaskId())
                    .setJobType(currentTask.getJobType())
                    .setTaskId(cassandraNodeTask.getTaskId())
                    .setStartedTimestamp(clock.now().getMillis())
                    .build();
            jobsState.nextNode(currentTask, currentNode);

            LOGGER.info("Starting cluster job {} on {}/{}", currentTask.getJobType().name(), nextNode.get().getIp(),
                    nextNode.get().getHostname());
        }
    }

    public void onNodeJobStatus(SlaveStatusDetails statusDetails) {
        ClusterJobStatus currentTask = getCurrentClusterJob();
        if (currentTask == null) {
            return;
        }

        if (!statusDetails.hasNodeJobStatus()) {
            // TODO add some failure handling here
            return;
        }

        NodeJobStatus nodeJobStatus = statusDetails.getNodeJobStatus();

        if (currentTask.getJobType() != nodeJobStatus.getJobType()) {
            // oops - status message of other type...  ignore for now
            LOGGER.warn("Got node job status of tye {} - but expected {}", nodeJobStatus.getJobType(), currentTask.getJobType());
            return;
        }

        LOGGER.info("Got node job status from {}, running={}", nodeJobStatus.getExecutorId(), nodeJobStatus.getRunning());

        jobsState.updateNodeStatus(currentTask, nodeJobStatus);
    }

    public boolean startClusterTask(ClusterJobType jobType) {
        if (jobsState.get().hasCurrentClusterJob())
            return false;

        ClusterJobStatus.Builder builder = ClusterJobStatus.newBuilder()
                .setJobType(jobType)
                .setStartedTimestamp(clock.now().getMillis());

        for (CassandraNode cassandraNode : clusterState.nodes()) {
            if (cassandraNode.hasCassandraNodeExecutor()) {
                builder.addRemainingNodes(cassandraNode.getCassandraNodeExecutor().getExecutorId());
            }
        }

        jobsState.currentJob(builder.build());
        return true;
    }

    public boolean abortClusterJob(ClusterJobType jobType) {
        ClusterJobStatus current = getCurrentClusterJob(jobType);
        if (current == null || current.getAborted())
            return false;

        current = ClusterJobStatus.newBuilder(current)
                .setAborted(true).build();
        jobsState.currentJob(current);
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
        CassandraNode cassandraNode = findNode(node);
        if (cassandraNode == null || cassandraNode.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
            return cassandraNode;
        }

        cassandraNode = CassandraFrameworkProtos.CassandraNode.newBuilder(cassandraNode)
            .setTargetRunState(CassandraNode.TargetRunState.STOP)
            .build();
        clusterState.addOrSetNode(cassandraNode);

        return cassandraNode;
    }

    public CassandraNode nodeRun(String node) {
        CassandraNode cassandraNode = findNode(node);
        if (cassandraNode == null || cassandraNode.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
            return cassandraNode;
        }

        cassandraNode = CassandraFrameworkProtos.CassandraNode.newBuilder(cassandraNode)
            .setTargetRunState(CassandraNode.TargetRunState.RUN)
            .build();
        clusterState.addOrSetNode(cassandraNode);

        return cassandraNode;
    }

    public CassandraNode nodeRestart(String node) {
        CassandraNode cassandraNode = findNode(node);
        if (cassandraNode == null || cassandraNode.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
            return cassandraNode;
        }

        cassandraNode = CassandraFrameworkProtos.CassandraNode.newBuilder(cassandraNode)
            .setTargetRunState(CassandraNode.TargetRunState.RESTART)
            .build();
        clusterState.addOrSetNode(cassandraNode);

        return cassandraNode;
    }

    public CassandraNode nodeTerminate(String node) {
        CassandraNode cassandraNode = findNode(node);
        if (cassandraNode == null) {
            return null;
        }

        cassandraNode = CassandraFrameworkProtos.CassandraNode.newBuilder(cassandraNode)
            .setTargetRunState(CassandraNode.TargetRunState.TERMINATE)
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
        if (total == 0)
            return Collections.emptyList();

        int totalLive = 0;
        for (int i = 0; i < total; i++) {
            if (isLiveNode(state.getNodes(i)))
                totalLive++;
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
                misses ++;
            }
        }

        return result;
    }

    public boolean isLiveNode(CassandraNode node) {
        if (!node.hasCassandraNodeExecutor())
            return false;
        if (getTaskForNode(node, CassandraNodeTask.TaskType.SERVER) == null)
            return false;
        HealthCheckHistoryEntry hc = lastHealthCheck(node.getCassandraNodeExecutor().getExecutorId());
        if (hc == null || !hc.hasDetails())
            return false;
        HealthCheckDetails hcd = hc.getDetails();
        if (!hcd.getHealthy() || !hcd.hasInfo())
            return false;
        NodeInfo info = hcd.getInfo();
        if (!info.hasNativeTransportRunning() || !info.hasRpcServerRunning())
            return false;
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
}
