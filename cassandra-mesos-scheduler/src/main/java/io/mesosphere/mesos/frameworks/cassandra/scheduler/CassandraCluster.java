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
import org.jetbrains.annotations.Nullable;
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

    private static final TaskResources EXECUTOR_RESOURCES = taskResources(0.1, 384, 256);
    private static final TaskResources METADATA_TASK_RESOURCES = taskResources(0.1, 32, 0);

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

    public static int getPortMapping(@NotNull final CassandraFrameworkConfiguration configuration, @NotNull final String name) {
        for (final PortMapping portMapping : configuration.getPortMappingList()) {
            if (portMapping.getName().equals(name)) {
                return portMapping.getPort();
            }
        }
        final Long port = defaultPortMappings.get(name);
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

    @Nullable
    public ExecutorMetadata metadataForExecutor(@NotNull final String executorId) {
        for (final ExecutorMetadata executorMetadata : clusterState.executorMetadata()) {
            if (executorId.equals(executorMetadata.getExecutorId())) {
                return executorMetadata;
            }
        }
        return null;
    }

    public void removeTask(@NotNull final String taskId, @NotNull final Protos.TaskStatus status) {
        final List<CassandraNode> nodes = clusterState.nodes();
        final List<CassandraNode> newNodes = new ArrayList<>(nodes.size());
        boolean changed = false;
        for (final CassandraNode cassandraNode : nodes) {
            final CassandraNodeTask nodeTask = CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, taskId);
            if (nodeTask == null) {
                newNodes.add(cassandraNode);
                continue;
            }
            final CassandraNode.Builder builder = CassandraFrameworkProtosUtils.removeTask(cassandraNode, nodeTask);
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

        final ClusterJobStatus clusterJob = getCurrentClusterJob();
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

    @Nullable
    public HealthCheckHistoryEntry lastHealthCheck(@NotNull final String executorId) {
        return healthCheckHistory.last(executorId);
    }

    public void recordHealthCheck(@NotNull final String executorId, @NotNull final HealthCheckDetails details) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("> recordHealthCheck(executorId : {}, details : {})", executorId, protoToString(details));
        }
        final Optional<CassandraNode> nodeOpt = cassandraNodeForExecutorId(executorId);
        if (nodeOpt.isPresent()) {
            if (!details.getHealthy()) {
                LOGGER.info(
                    "health check result unhealthy for node: {}. Message: '{}'",
                    nodeOpt.get().hasCassandraNodeExecutor() ? nodeOpt.get().getCassandraNodeExecutor().getExecutorId() : nodeOpt.get().getHostname(),
                    details.getMsg()
                );
            } else {
                // upon the first healthy response clear the replacementForIp field
                final CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(nodeOpt.get(), CassandraNodeTask.NodeTaskType.SERVER);
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
        final List<CassandraNode> nodes = new ArrayList<>();
        for (final CassandraNode n : clusterState.nodes()) {
            if (n.getSeed() && isLiveNode(n)) {
                nodes.add(n);
            }
        }
        return nodes;
    }

    @Nullable
    public TasksForOffer getTasksForOffer(@NotNull final Protos.Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(marker, "> getTasksForOffer(offer : {})", protoToString(offer));
        }

        try {
            return _getTasksForOffer(marker, offer);
        } finally {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(marker, "< getTasksForOffer(offer : {}) = {}, {}", protoToString(offer));
            }
        }
    }

    @NotNull
    private static String executorTaskId(@NotNull final CassandraNode.Builder node) {
        return node.getCassandraNodeExecutor().getExecutorId();
    }

    @NotNull
    private static String configUpdateTaskId(@NotNull final CassandraNode.Builder node) {
        return executorTaskId(node) + ".config";
    }

    @NotNull
    private String serverTaskName() {
        return configuration.frameworkName() + ".node";
    }

    @NotNull
    private static String serverTaskId(@NotNull final CassandraNode.Builder node) {
        return executorTaskId(node) + ".server";
    }

    public long nextPossibleServerLaunchTimestamp() {
        return nextPossibleServerLaunchTimestamp(
            getClusterState().get().getLastServerLaunchTimestamp(),
            getConfiguration().get().getBootstrapGraceTimeSeconds(),
            getConfiguration().get().getHealthCheckIntervalSeconds()
        );
    }

    @NotNull
    public Optional<CassandraNode> cassandraNodeForHostname(@NotNull final String hostname) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeHostnameEq(hostname))
        );
    }

    @NotNull
    public Optional<CassandraNode> cassandraNodeForExecutorId(@NotNull final String executorId) {
        return headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeExecutorIdEq(executorId))
        );
    }

    @NotNull
    private CassandraNode buildCassandraNode(@NotNull final Protos.Offer offer, final boolean seed, @Nullable final String replacementForIp) {
        final CassandraNode.Builder builder = CassandraNode.newBuilder()
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
            final InetAddress ia = InetAddress.getByName(offer.getHostname());

            int jmxPort = getPortMapping(PORT_JMX);
            if (ia.isLoopbackAddress()) {
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    jmxPort = serverSocket.getLocalPort();
                } catch (final IOException e) {
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
        } catch (final UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private int getPortMapping(@NotNull final String name) {
        return getPortMapping(configuration.get(), name);
    }

    @NotNull
    private static Map<String, Long> portMappings(@NotNull final CassandraFrameworkConfiguration config) {
        final Map<String, Long> r = new HashMap<>();
        for (final String name : defaultPortMappings.keySet()) {
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
        @NotNull final ExecutorMetadata metadata
    ) {
        final CassandraFrameworkConfiguration config = configuration.get();
        final CassandraConfigRole configRole = config.getDefaultConfigRole();

        final CassandraServerConfig cassandraServerConfig = buildCassandraServerConfig(metadata, config, configRole, TaskEnv.getDefaultInstance());

        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setType(TaskDetails.TaskDetailsType.UPDATE_CONFIG)
            .setUpdateConfigTask(UpdateConfigTask.newBuilder()
                    .setCassandraServerConfig(cassandraServerConfig)
            )
            .build();

        return CassandraNodeTask.newBuilder()
            .setType(CassandraNodeTask.NodeTaskType.CONFIG)
            .setTaskId(taskId)
            .setResources(METADATA_TASK_RESOURCES)
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    private CassandraNodeTask getServerTask(
        @NotNull final String taskId,
        @NotNull final String taskName,
        @NotNull final ExecutorMetadata metadata,
        @NotNull final CassandraNode.Builder node
    ) {
        final CassandraFrameworkConfiguration config = configuration.get();
        final CassandraConfigRole configRole = config.getDefaultConfigRole();

        final TaskEnv.Builder taskEnv = TaskEnv.newBuilder();
        for (final TaskEnv.Entry entry : configRole.getTaskEnv().getVariablesList()) {
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

        final ArrayList<String> command = newArrayList("apache-cassandra-" + configRole.getCassandraVersion() + "/bin/cassandra", "-f");
        if (node.hasReplacementForIp()) {
            command.add("-Dcassandra.replace_address=" + node.getReplacementForIp());
        }

        final CassandraServerConfig cassandraServerConfig = buildCassandraServerConfig(metadata, config, configRole, taskEnv.build());

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
                    .setHealthCheckIntervalSeconds(configuration.healthCheckInterval().toDuration().getStandardSeconds())
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
        @NotNull final ExecutorMetadata metadata,
        @NotNull final CassandraFrameworkConfiguration config,
        @NotNull final CassandraConfigRole configRole,
        @NotNull final TaskEnv taskEnv
    ) {
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
        final String osName = Env.option("OS_NAME").or(Env.osFromSystemProperty());
        final String javaExec = "macosx".equals(osName)
            ? "$(pwd)/jre*/Contents/Home/bin/java"
            : "$(pwd)/jre*/bin/java";

        final CassandraConfigRole configRole = configuration.getDefaultConfigRole();

        final List<String> command = newArrayList(
            javaExec,
//            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
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
            .setResources(EXECUTOR_RESOURCES)
            .addAllDownload(newArrayList(
                resourceFileDownload(getUrlForResource("/jre-7-" + osName + ".tar.gz"), true),
                resourceFileDownload(getUrlForResource("/apache-cassandra-" + configRole.getCassandraVersion() + "-bin.tar.gz"), true),
                resourceFileDownload(getUrlForResource("/cassandra-executor.jar"), false)
            ))
            .build();
    }

    @NotNull
    private static CassandraNodeTask getMetadataTask(@NotNull final String executorId, @NotNull final String ip) {
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
            .setResources(METADATA_TASK_RESOURCES)
            .setTaskDetails(taskDetails)
            .build();
    }

    @NotNull
    static List<String> hasResources(
        @NotNull final Protos.Offer offer,
        @NotNull final TaskResources resources,
        @NotNull final Map<String, Long> portMapping,
        @NotNull final String mesosRole
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

    public int updateNodeCount(final int nodeCount) {
        final int currentDesiredNodeCount = configuration.targetNumberOfNodes();
        if (nodeCount < currentDesiredNodeCount) {
            throw new IllegalArgumentException("Can not decrease the number of nodes.");
        } else if (nodeCount > currentDesiredNodeCount) {
            configuration.targetNumberOfNodes(nodeCount);
        }
        return nodeCount;
    }

    // cluster tasks

    private void handleClusterTask(@NotNull final String executorId, @NotNull final TasksForOffer tasksForOffer) {
        final ClusterJobStatus currentJob = getCurrentClusterJob();
        if (currentJob == null) {
            return;
        }

        final Optional<CassandraNode> nodeForExecutorId = cassandraNodeForExecutorId(executorId);

        clusterJobHandlers.get(currentJob.getJobType()).handleTaskOffer(currentJob, executorId, nodeForExecutorId, tasksForOffer);
    }

    public void onNodeJobStatus(@NotNull final SlaveStatusDetails statusDetails) {
        final ClusterJobStatus currentJob = getCurrentClusterJob();
        if (currentJob == null) {
            return;
        }

        if (!statusDetails.hasNodeJobStatus()) {
            // TODO add some failure handling here
            return;
        }

        final NodeJobStatus nodeJobStatus = statusDetails.getNodeJobStatus();

        if (currentJob.getJobType() != nodeJobStatus.getJobType()) {
            // oops - status message of other type...  ignore for now
            LOGGER.warn("Got node job status of tye {} - but expected {}", nodeJobStatus.getJobType(), currentJob.getJobType());
            return;
        }

        clusterJobHandlers.get(currentJob.getJobType()).onNodeJobStatus(currentJob, nodeJobStatus);
    }

    public boolean startClusterTask(@NotNull final ClusterJobType jobType) {
        if (jobsState.get().hasCurrentClusterJob()) {
            return false;
        }

        final ClusterJobStatus.Builder builder = ClusterJobStatus.newBuilder()
                .setJobType(jobType)
                .setStartedTimestamp(clock.now().getMillis());

        for (final CassandraNode cassandraNode : clusterState.nodes()) {
            if (cassandraNode.hasCassandraNodeExecutor()) {
                builder.addRemainingNodes(cassandraNode.getCassandraNodeExecutor().getExecutorId());
            }
        }

        jobsState.setCurrentJob(builder.build());

        return true;
    }

    public boolean abortClusterJob(@NotNull final ClusterJobType jobType) {
        ClusterJobStatus current = getCurrentClusterJob(jobType);
        if (current == null || current.getAborted()) {
            return false;
        }

        current = ClusterJobStatus.newBuilder(current)
                .setAborted(true).build();
        jobsState.setCurrentJob(current);
        return true;
    }

    @Nullable
    public ClusterJobStatus getCurrentClusterJob() {
        final CassandraClusterJobs jobState = jobsState.get();
        return jobState.hasCurrentClusterJob() ? jobState.getCurrentClusterJob() : null;
    }

    @Nullable
    public ClusterJobStatus getCurrentClusterJob(@NotNull final ClusterJobType jobType) {
        final ClusterJobStatus current = getCurrentClusterJob();
        return current != null && current.getJobType() == jobType ? current : null;
    }

    @Nullable
    public ClusterJobStatus getLastClusterJob(@NotNull final ClusterJobType jobType) {
        final List<ClusterJobStatus> list = jobsState.get().getLastClusterJobsList();
        if (list == null) {
            return null;
        }
        for (int i = list.size() - 1; i >= 0; i--) {
            final ClusterJobStatus clusterJobStatus = list.get(i);
            if (clusterJobStatus.getJobType() == jobType) {
                return clusterJobStatus;
            }
        }
        return null;
    }

    @Nullable
    public CassandraNode findNode(@NotNull final String node) {
        for (final CassandraNode cassandraNode : clusterState.nodes()) {
            if (cassandraNode.getIp().equals(node)
                || cassandraNode.getHostname().equals(node)
                || (cassandraNode.hasCassandraNodeExecutor() && cassandraNode.getCassandraNodeExecutor().getExecutorId().equals(node))) {
                return cassandraNode;
            }
        }
        return null;
    }

    @Nullable
    public CassandraNode nodeStop(final String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.STOP);
    }

    @Nullable
    public CassandraNode nodeRun(final String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.RUN);
    }

    @Nullable
    public CassandraNode nodeRestart(final String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.RESTART);
    }

    @Nullable
    public CassandraNode nodeTerminate(final String node) {
        return updateNodeTargetRunState(node, CassandraNode.TargetRunState.TERMINATE);
    }

    @Nullable
    public CassandraNode updateNodeTargetRunState(final String node, final CassandraNode.TargetRunState targetRunState) {
        final CassandraNode cassandraNode = findNode(node);
        return updateNodeTargetRunState(cassandraNode, targetRunState);
    }

    @Nullable
    public CassandraNode updateNodeTargetRunState(@Nullable CassandraNode cassandraNode, @NotNull final CassandraNode.TargetRunState targetRunState) {
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

    public void updateCassandraProcess(@NotNull final Protos.ExecutorID executorId, @NotNull final CassandraServerRunMetadata cassandraServerRunMetadata) {
        final Optional<CassandraNode> node = cassandraNodeForExecutorId(executorId.getValue());
        if (node.isPresent()) {
            clusterState.addOrSetNode(CassandraFrameworkProtos.CassandraNode.newBuilder(node.get())
                .setCassandraDaemonPid(cassandraServerRunMetadata.getPid())
                .build());
        }
    }

    @NotNull
    public List<CassandraNode> liveNodes(int limit) {
        final CassandraClusterState state = clusterState.get();
        final int total = state.getNodesCount();
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

        final ThreadLocalRandom tlr = ThreadLocalRandom.current();
        final List<CassandraNode> result = new ArrayList<>(limit);
        int misses = 0;
        while (result.size() < limit && misses < 250) { // the check for 250 misses is a poor-man's implementation to prevent a possible race-condition
            final int i = tlr.nextInt(total);
            final CassandraNode node = state.getNodes(i);
            if (isLiveNode(node) && !result.contains(node)) {
                result.add(node);
                misses = 0;
            } else {
                misses++;
            }
        }

        return result;
    }

    public boolean isLiveNode(@NotNull final CassandraNode node) {
        if (!node.hasCassandraNodeExecutor()) {
            return false;
        }
        if (getTaskForNode(node, CassandraNodeTask.NodeTaskType.SERVER) == null) {
            return false;
        }
        final HealthCheckHistoryEntry hc = lastHealthCheck(node.getCassandraNodeExecutor().getExecutorId());
        return isLiveNode(hc);
    }

    public boolean isLiveNode(@Nullable final HealthCheckHistoryEntry hc) {
        if (hc == null || !hc.hasDetails()) {
            return false;
        }
        final HealthCheckDetails hcd = hc.getDetails();
        if (!hcd.getHealthy() || !hcd.hasInfo()) {
            return false;
        }
        final NodeInfo info = hcd.getInfo();
        if (!info.hasNativeTransportRunning() || !info.hasRpcServerRunning()) {
            return false;
        }
        return info.getNativeTransportRunning() && info.getRpcServerRunning();
    }

    @NotNull
    public CassandraNode replaceNode(@NotNull final String node) throws ReplaceNodePreconditionFailed {
        final CassandraNode cassandraNode = findNode(node);
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
    public List<String> getNodeLogFiles(@NotNull final CassandraNode cassandraNode) {

        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata = metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
        if (executorMetadata == null) {
            return Collections.emptyList();
        }

        final String workdir = executorMetadata.getWorkdir();
        return newArrayList(
            workdir + "/executor.log",
            workdir + "/apache-cassandra-" + getConfiguration().getDefaultConfigRole().getCassandraVersion() + "/logs/system.log");
    }

    public boolean setNodeSeed(@NotNull final CassandraNode cassandraNode, final boolean seed) throws SeedChangeException {
        if (cassandraNode.getSeed() == seed) {
            return false;
        }

        final List<CassandraNode> liveSeedNodes = getLiveSeedNodes();

        if (clusterState.nodeCounts().getSeedCount() < configuration.targetNumberOfSeeds()) {
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

    public static int numberOfNodesToAcquire(final NodeCounts nodeCounts, final PersistedCassandraFrameworkConfiguration configuration) {
        return configuration.targetNumberOfNodes() - nodeCounts.getNodeCount();
    }

    public static int numberOfSeedsToAcquire(final NodeCounts nodeCounts, final PersistedCassandraFrameworkConfiguration configuration) {
        return configuration.targetNumberOfSeeds() - nodeCounts.getSeedCount();
    }

    /**
     * Basic algorithm used when evaluating offers
     * <ol>
     *     <li>
     *         Do we think there is a node for the host?<br/>
     *         If not:
     *         <ol>
     *             <li>
     *                 Do we need to acquire another node?<br/>
     *                 If not, return {@code null}.<br/>
     *                 If we do, collect the following info before constructing the node:
     *                 <ol>
     *                     <li>Ip address of node to be replaced, if any</li>
     *                     <li>Is the node to act as a seed?</li>
     *                 </ol>
     *             </li>
     *         </ol>
     *     </li>
     *
     *     <li>
     *         Do we think there is already an executor for the node?<br/>
     *         If not:
     *         <ol>
     *             <li>Is the nodes target run state {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#TERMINATE TERMINATE}, if so return {@code null}</li>
     *             <li>Generate an executorId and construct the new executor.</li>
     *             <li>Set the new executor to the node</li>
     *         </ol>
     *     </li>
     *
     *     <li>
     *         Do we think there is already a {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType#METADATA METADATA}
     *         running on the node?<br/>
     *         If not:
     *         <ol>
     *             <li>Is the nodes target run state {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#TERMINATE TERMINATE}, if so return {@code null}</li>
     *             <li>Construct a metadata task, add it to the nodes tasks add it to the list of tasks to launch</li>
     *         </ol>
     *
     *         If it does:
     *         <ol>
     *             <li>Lookup the metadata that has been collected for the node</li>
     *             If present:
     *             <ol>
     *                 <li>If a "Cluster Job is running" call it's {@link ClusterJobHandler#handleTaskOffer handleTaskOffer} method</li>
     *                 <li>
     *                     Do we think there is already a {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType#SERVER}
     *                     task running?
     *
     *                     If not:
     *                     <ol>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#RUN RUN}, proceed.</li>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#TERMINATE TERMINATE} send a task kill message and return from the method</li>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#STOP STOP} return {@code null}</li>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#RESTART RESTART} set the node target run state to {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#RUN RUN}</li>
     *                         <li>If we are still trying to acquire seed nodes return {@code null}</li>
     *                         <li>If the time between server node launches hasn't elapsed return {@code null}</li>
     *                         <li>
     *                             If node is not intended to be a seed:
     *                             <ol>
     *                                 <li>Check to make sure that a seed node is up and "healthy"</li>
     *                             </ol>
     *                         </li>
     *                         <li>
     *                             Evaluate if the resource offer has enough resources to be able to run the server
     *                             <ol>
     *                                 <li>If there aren't enough resources log a message to indicate the insufficient resources</li>
     *                                 <li>
     *                                     If there are enough resources
     *                                     <ol>
     *                                         <li>Get host metadata</li>
     *                                         <li>Construct new server task and add it to the nodes tasks add it to the list of tasks to launch</li>
     *                                         <li>Update last server launch time</li>
     *                                     </ol>
     *                                 </li>
     *                             </ol>
     *                         </li>
     *                     </ol>
     *
     *                     If we do:
     *                     <ol>
     *                         <li>If config update required, create config update task and add it to the nodes tasks add it to the list of tasks to launch</li>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#RUN RUN} and we need to run a health check, queue a framework message to trigger a health check.</li>
     *                         <li>If target run state is {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#STOP STOP}, {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#RESTART RESTART} or {@link io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode.TargetRunState#TERMINATE TERMINATE} send a task kill message and return from the method</li>
     *                     </ol>
     *                 </li>
     *             </ol>
     *
     *             Otherwise wait until the next offer to try and proceed
     *         </ol>
     *     </li>
     *
     * </ol>
     *
     */
    @Nullable
    @VisibleForTesting
    TasksForOffer _getTasksForOffer(@NotNull final Marker marker, final @NotNull Protos.Offer offer) {
        final Optional<CassandraNode> nodeOption = cassandraNodeForHostname(offer.getHostname());
        final CassandraConfigRole configRole = configuration.getDefaultConfigRole();
        final CassandraFrameworkConfiguration config = configuration.get();

        final NodeCounts nodeCounts = clusterState.nodeCounts();
        final boolean allSeedsAcquired = nodeCounts.getSeedCount() >= configuration.targetNumberOfSeeds();

        final long now = clock.now().getMillis();
        final long nextPossibleServerLaunchTimestamp = nextPossibleServerLaunchTimestamp();
        final boolean canLaunchServerTask = canLaunchServerTask(now, nextPossibleServerLaunchTimestamp);

        final CassandraNode.Builder node;
        if (!nodeOption.isPresent()) {
            if (nodeCounts.getNodeCount() >= configuration.targetNumberOfNodes()) {
                LOGGER.debug(marker, "Number of desired Cassandra Nodes Acquired, no new node to launch.");
                // number of C* cluster nodes already present
                return null;
            }

            if (allSeedsAcquired && !canLaunchServerTask) {
                final long nextPossibleServerLaunchSeconds = secondsUntilNextPossibleServerLaunch(now, nextPossibleServerLaunchTimestamp);
                LOGGER.info(marker, "Preventing creation of new node because server launch timeout active. Next server launch possible in {}s", nextPossibleServerLaunchSeconds);
                return null;
            }

            final TaskResources allResources = add(
                add(EXECUTOR_RESOURCES, METADATA_TASK_RESOURCES),
                configRole.getResources()
            );
            final List<String> executorSizeErrors = hasResources(
                offer,
                allResources,
                portMappings(config),
                configRole.getMesosRole()
            );
            if (!executorSizeErrors.isEmpty()) {
                // there aren't enough resources to even attempt to run the server, skip this host for now.
                LOGGER.info(
                    marker,
                    "Insufficient resources in offer for executor, not attempting to launch new node. Details for offer {}: ['{}']",
                    offer.getId().getValue(), JOINER.join(executorSizeErrors)
                );
                return null;
            }

            final String replacementForIp = clusterState.nextReplacementIp();

            final CassandraNode newNode = buildCassandraNode(offer, !allSeedsAcquired, replacementForIp);
            clusterState.nodeAcquired(newNode);
            node = CassandraNode.newBuilder(newNode);
        } else {
            node = CassandraNode.newBuilder(nodeOption.get());
        }

        if (!node.hasCassandraNodeExecutor()) {
            if (node.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
                // node completely terminated
                LOGGER.info(marker, "Node marked for Termination, not launching any tasks.");
                return null;
            }
            final String executorId = getExecutorIdForOffer(offer);
            LOGGER.debug(marker, "Configuring new executor {}", executorId);
            final CassandraNodeExecutor executor = buildCassandraNodeExecutor(executorId);
            node.setCassandraNodeExecutor(executor);
        }

        final CassandraNodeExecutor executor = node.getCassandraNodeExecutor();

        final TasksForOffer result = new TasksForOffer(executor);
        final String executorId = executor.getExecutorId();
        CassandraNodeTask metadataTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.NodeTaskType.METADATA);
        if (metadataTask == null) {
            if (node.getTargetRunState() == CassandraNode.TargetRunState.TERMINATE) {
                // node completely terminated
                LOGGER.info(marker, "Node marked for Termination, not launching any tasks.");
                return null;
            }
            LOGGER.info(marker, "Launching new metadata task for executor: {}", executorId);
            metadataTask = getMetadataTask(executorId, node.getIp());
            node.addTasks(metadataTask);
            result.getLaunchTasks().add(metadataTask);
        } else {
            final Optional<ExecutorMetadata> maybeMetadata = getExecutorMetadata(executorId);
            if (!maybeMetadata.isPresent()) {
                LOGGER.debug(marker, "Metadata for node has not been resolved can not launch server task.");
            } else {
                handleClusterTask(executorId, result);

                final CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraNodeTask.NodeTaskType.SERVER);
                if (serverTask == null) {
                    switch (node.getTargetRunState()) {
                        case RUN:
                            LOGGER.debug(marker, "Attempting to launch server task for node.");
                            break;
                        case TERMINATE:

                            LOGGER.info(marker, "Killing executor {}", executorTaskId(node));
                            result.getKillTasks().add(Protos.TaskID.newBuilder().setValue(executorTaskId(node)).build());

                            return result;
                        case STOP:
                            LOGGER.info(marker, "Cannot launch server (targetRunState==STOP)");
                            return null;
                        case RESTART:
                            // change state to run (RESTART complete)
                            node.setTargetRunState(CassandraNode.TargetRunState.RUN);
                            break;
                    }

                    if (!allSeedsAcquired) {
                        // we do not have enough executor metadata records to fulfil seed node requirement
                        LOGGER.info(marker, "Cannot launch non-seed node (seed node requirement not fulfilled)");
                        return null;
                    }

                    if (!canLaunchServerTask) {
                        final long nextPossibleServerLaunchSeconds = secondsUntilNextPossibleServerLaunch(now, nextPossibleServerLaunchTimestamp);
                        LOGGER.info(marker, "Server launch timeout active. Next server launch possible in {}s", nextPossibleServerLaunchSeconds);
                        return null;
                    }

                    if (!node.getSeed()) {
                        // when starting a non-seed node also check if at least one seed node is running
                        // (otherwise that node will fail to start)
                        boolean anySeedRunning = anySeedRunningAndHealthy();
                        if (!anySeedRunning) {
                            LOGGER.info(marker, "Cannot start server task because no seed node is running");
                            return null;
                        }
                    }

                    final List<String> errors = hasResources(
                        offer,
                        configRole.getResources(),
                        portMappings(config),
                        configRole.getMesosRole()
                    );
                    if (!errors.isEmpty()) {
                        LOGGER.info(marker, "Insufficient resources in offer for server: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                    } else {
                        final ExecutorMetadata metadata = maybeMetadata.get();
                        final CassandraNodeTask task = getServerTask(serverTaskId(node), serverTaskName(), metadata, node);
                        node.addTasks(task)
                            .setNeedsConfigUpdate(false);
                        result.getLaunchTasks().add(task);

                        clusterState.updateLastServerLaunchTimestamp(now);
                    }
                } else {
                    LOGGER.debug(marker, "Server task for node already running.");
                    if (node.getNeedsConfigUpdate()) {
                        LOGGER.info(marker, "Launching config update tasks for executor: {}", executorId);
                        final CassandraNodeTask task = getConfigUpdateTask(configUpdateTaskId(node), maybeMetadata.get());
                        node.addTasks(task)
                            .setNeedsConfigUpdate(false);
                        result.getLaunchTasks().add(task);
                    }

                    switch (node.getTargetRunState()) {
                        case RUN:
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
            LOGGER.info(marker, "No tasks to launch.");
            return null;
        }

        final CassandraNode built = node.build();
        clusterState.addOrSetNode(built);

        return result;
    }

    private boolean anySeedRunningAndHealthy() {
        boolean anySeedRunning = false;
        for (final CassandraNode cassandraNode : clusterState.nodes()) {
            if (CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, CassandraNodeTask.NodeTaskType.SERVER) != null) {
                final HealthCheckHistoryEntry lastHC = lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                if (cassandraNode.getSeed()) {
                    if (lastHC != null && isNodeHealthyJoinedAndOperatingNormally(lastHC)) {
                        anySeedRunning = true;
                    }
                }
            }
        }
        return anySeedRunning;
    }

    @NotNull
    private static TaskResources taskResources(final double cpuCores, final long memMb, final long diskMb) {
        return TaskResources.newBuilder()
            .setCpuCores(cpuCores)
            .setMemMb(memMb)
            .setDiskMb(diskMb)
            .build();
    }

    private static boolean isNodeHealthyJoinedAndOperatingNormally(@NotNull final HealthCheckHistoryEntry lastHC) {
        return lastHC.getDetails() != null
            && lastHC.getDetails().getInfo() != null
            && lastHC.getDetails().getHealthy()
            && lastHC.getDetails().getInfo().getJoined()
            && "NORMAL".equals(lastHC.getDetails().getInfo().getOperationMode());
    }

    private static TaskResources add(@NotNull final TaskResources r1, @NotNull final TaskResources r2) {
        return TaskResources.newBuilder()
            .setCpuCores(r1.getCpuCores() + r2.getCpuCores())
            .setMemMb(r1.getMemMb() + r2.getMemMb())
            .setDiskMb(r1.getDiskMb() + r2.getDiskMb())
            .build();
    }

    @VisibleForTesting
    static long nextPossibleServerLaunchTimestamp(
        final long lastServerLaunchTimestamp,
        final long bootstrapGraceTimeSeconds,
        final long healthCheckIntervalSeconds
    ) {
        final long seconds = Math.max(bootstrapGraceTimeSeconds, healthCheckIntervalSeconds);
        return lastServerLaunchTimestamp + seconds * 1000L;
    }

    @VisibleForTesting
    static boolean canLaunchServerTask(final long now, final long nextPossibleServerLaunchTimestamp) {
        return now >= nextPossibleServerLaunchTimestamp;
    }

    @VisibleForTesting
    static long secondsUntilNextPossibleServerLaunch(final long now, final long nextPossibleServerLaunchTimestamp) {
        final long millisUntilNext = nextPossibleServerLaunchTimestamp - now;
        if (millisUntilNext <= 0) {
            return 0L;
        }
        return millisUntilNext / 1000;
    }
}
