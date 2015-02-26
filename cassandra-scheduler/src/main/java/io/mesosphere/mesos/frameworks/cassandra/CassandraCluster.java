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

    private static CassandraCluster singleton;

    public static CassandraCluster singleton() {
        return singleton;
    }

    public static int getPortMapping(CassandraFrameworkConfiguration configuration, String name) {
        for (PortMapping portMapping : configuration.getPortMappingList()) {
            if (portMapping.getName().equals(name))
                return portMapping.getPort();
        }
        Long port = defaultPortMappings.get(name);
        if (port == null)
            throw new IllegalStateException("no port mapping for " + name);
        return port.intValue();
    }

    public CassandraCluster(
        @NotNull final Clock clock,
        @NotNull final String httpServerBaseUrl,
        @NotNull final ExecutorCounter execCounter,
        @NotNull final PersistedCassandraClusterState clusterState,
        @NotNull final PersistedCassandraClusterHealthCheckHistory healthCheckHistory,
        @NotNull final PersistedCassandraFrameworkConfiguration configuration
    ) {
        singleton = this;
        this.clock = clock;
        this.httpServerBaseUrl = httpServerBaseUrl;
        this.execCounter = execCounter;
        this.clusterState = clusterState;
        this.healthCheckHistory = healthCheckHistory;
        this.configuration = configuration;
    }

    @NotNull
    public CassandraClusterState getClusterState() {
        return clusterState.get();
    }

    @NotNull
    public CassandraFrameworkConfiguration getConfiguration() {
        return configuration.get();
    }

    public void removeTask(@NotNull final String taskId) {
        final FluentIterable<CassandraNode> update = from(clusterState.nodes())
            .transform(cassandraNodeToBuilder())
            .transform(new ContinuingTransform<CassandraNode.Builder>() {
                @Override
                public CassandraNode.Builder apply(final CassandraNode.Builder input) {
                    if (input.hasMetadataTask() && input.getMetadataTask().getTaskId().equals(taskId)) {
                        return input.clearMetadataTask();
                    }
                    return input;
                }
            })
            .transform(new ContinuingTransform<CassandraNode.Builder>() {
                @Override
                public CassandraNode.Builder apply(final CassandraNode.Builder input) {
                    if (input.hasServerTask() && input.getServerTask().getTaskId().equals(taskId)) {
                        return input.clearServerTask();
                    }
                    return input;
                }
            })
            .transform(cassandraNodeBuilderToCassandraNode());
        clusterState.nodes(newArrayList(update));
    }

    public void removeExecutor(@NotNull final String executorId) {
        final FluentIterable<CassandraNode> update = from(clusterState.nodes())
            .transform(cassandraNodeToBuilder())
            .transform(new ContinuingTransform<CassandraNode.Builder>() {
                @Override
                public CassandraNode.Builder apply(final CassandraNode.Builder input) {
                    if (input.hasCassandraNodeExecutor() && input.getCassandraNodeExecutor().getExecutorId().equals(executorId)) {
                        return input.clearMetadataTask()
                            .clearServerTask();
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

    public void addExecutorMetadata(@NotNull final ExecutorMetadata executorMetadata) {
        clusterState.executorMetadata(append(
                clusterState.executorMetadata(),
                executorMetadata
        ));
    }

    public void removeExecutorMetadata(@NotNull final String executorId) {
        final FluentIterable<ExecutorMetadata> update = from(clusterState.executorMetadata())
            .filter(not(new Predicate<ExecutorMetadata>() {
                @Override
                public boolean apply(final ExecutorMetadata input) {
                    return input.getExecutorId().equals(executorId);
                }
            }));
        clusterState.executorMetadata(newArrayList(update));
    }

    public HealthCheckHistoryEntry lastHealthCheck(@NotNull final String executorId) {
        return healthCheckHistory.last(executorId);
    }

    public void recordHealthCheck(@NotNull final String executorId, @NotNull final HealthCheckDetails details) {
        LOGGER.debug("> recordHealthCheck(executorId : {}, details : {})", executorId, protoToString(details));
        if (!details.getHealthy()) {
            final Optional<CassandraNode> nodeOpt = headOption(
                from(clusterState.nodes())
                    .filter(cassandraNodeExecutorIdEq(executorId))
            );
            if (nodeOpt.isPresent()) {
                LOGGER.info(
                    "health check result unhealthy for node: {}. Message: '{}'",
                    nodeOpt.get().getCassandraNodeExecutor().getExecutorId(),
                    details.getMsg()
                    );
                // TODO: This needs to be smarter, right not it assumes that as soon as it's unhealth it's dead
                //removeTask(nodeOpt.get().getServerTask().getTaskId());
            }
        }
        healthCheckHistory.record(
            HealthCheckHistoryEntry.newBuilder()
                .setExecutorId(executorId)
                .setTimestamp(clock.now().getMillis())
                .setDetails(details)
                .build()
        );
        LOGGER.debug("< recordHealthCheck(executorId : {}, details : {})", executorId, protoToString(details));
    }

    @NotNull
    public List<String> getSeedNodes() {
        return newArrayList(from(clusterState.executorMetadata()).transform(CassandraFrameworkProtosUtils.executorMetadataToIp()));
    }

    public CassandraNodeExecutor getTasksForOffer(@NotNull final Protos.Offer offer, List<CassandraNodeTask> launchTasks, List<TaskDetails> submitTasks) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        LOGGER.debug(marker, "> getTasksForOffer(offer : {})", protoToString(offer));

        final Optional<CassandraNode> nodeOption = headOption(
                from(clusterState.nodes())
                        .filter(cassandraNodeHostnameEq(offer.getHostname()))
        );

        CassandraNode.Builder node;
        if (!nodeOption.isPresent()) {
            if (!launchNode())
                return null;

            CassandraNode newNode = buildCassandraNode(offer);
            clusterState.nodes(append(
                    clusterState.nodes(),
                    newNode
            ));
            node = CassandraNode.newBuilder(newNode);
        }
        else
            node = CassandraNode.newBuilder(nodeOption.get());

        if (!node.hasCassandraNodeExecutor()) {
            final String executorId = getExecutorIdForOffer(offer);
            final CassandraNodeExecutor executor = getCassandraNodeExecutorSupplier(executorId);
            node.setCassandraNodeExecutor(executor);
        }

        final CassandraNodeExecutor executor = node.getCassandraNodeExecutor();
        final String executorId = executor.getExecutorId();
        if (!node.hasMetadataTask()) {
            // TODO add some grace time between two metadata-tasks (at least one minute to allow downloading, etc)

            final CassandraNodeTask metadataTask = getMetadataTask(executorId);
            node.setMetadataTask(metadataTask);
            launchTasks.add(metadataTask);
        } else {
            final Optional<ExecutorMetadata> maybeMetadata = getExecutorMetadata(executorId);
            if (maybeMetadata.isPresent()) {
                if (!node.hasServerTask()) {
                    CassandraFrameworkConfiguration config = configuration.get();
                    final List<String> errors = hasResources(
                            offer,
                            config.getCpuCores(),
                            config.getMemMb(),
                            config.getDiskMb(),
                            portMappings(config)
                    );
                    if (!errors.isEmpty()) {
                        LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                    } else {
                        final String taskId = executorId + ".server";
                        final ExecutorMetadata metadata = maybeMetadata.get();
                        final CassandraNodeTask task = getServerTask(executorId, taskId, metadata, node);
                        node.setServerTask(task);
                        launchTasks.add(task);
                    }
                } else if (shouldRunHealthCheck(executorId)) {
                    // TODO if a health check response is 'super-lost' then issue a health-check via SchedulerDriver.launchTasks() and check the result
                    // We have to recover state based on that response.
                    if (false) {
                        final String taskId = node.getCassandraNodeExecutor().getExecutorId() + ".healthcheck";
                        launchTasks.add(getHealthCheckTask(executorId, taskId));
                    } else {
                        submitTasks.add(getHealthCheckTaskDetails());
                    }
                } else {
                    return null;
                }
            }
        }

        if (submitTasks.isEmpty() && launchTasks.isEmpty())
            // nothing to do
            return null;

        final CassandraNode built = node.build();
        clusterState.addOrSetNode(built);

        LOGGER.trace(marker, "< getTasksForOffer(offer : {}) = {}, {}", protoToString(offer), protoToString(launchTasks), protoToString(submitTasks));
        return built.getCassandraNodeExecutor();
    }

    private CassandraNode buildCassandraNode(Protos.Offer offer) {
        CassandraNode.Builder builder = CassandraNode.newBuilder()
                .setHostname(offer.getHostname());
        try {
            InetAddress ia = InetAddress.getByName(offer.getHostname());

            int jmxPort = getPortMapping(PORT_JMX);
            if (ia.isLoopbackAddress())
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    jmxPort = serverSocket.getLocalPort();
                } catch (IOException e) {
                    throw new RuntimeException(e);
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

    private boolean shouldRunHealthCheck(@NotNull final String executorID) {
        final Optional<Long> previousHealthCheckTime = headOption(
                from(healthCheckHistory.entries())
                        .filter(healthCheckHistoryEntryExecutorIdEq(executorID))
                        .transform(healthCheckHistoryEntryToTimestamp())
                        .toSortedList(Collections.reverseOrder(naturalLongComparator))
        );
        if (previousHealthCheckTime.isPresent()) {
            final Duration duration = new Duration(new Instant(previousHealthCheckTime.get()), clock.now());
            return duration.isLongerThan(configuration.healthCheckInterval());
        } else {
            return true;
        }
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

    private boolean launchNode() {
        return clusterState.nodes().size() < configuration.numberOfNodes();
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
        final TaskConfig taskConfig = TaskConfig.newBuilder()
            .addVariables(configValue("cluster_name", config.getFrameworkName()))
            .addVariables(configValue("broadcast_address", metadata.getIp()))
            .addVariables(configValue("rpc_address", metadata.getIp()))
            .addVariables(configValue("listen_address", metadata.getIp()))
            .addVariables(configValue("storage_port", getPortMapping(config, PORT_STORAGE)))
            .addVariables(configValue("ssl_storage_port", getPortMapping(config, PORT_STORAGE_SSL)))
            .addVariables(configValue("native_transport_port", getPortMapping(config, PORT_NATIVE)))
            .addVariables(configValue("rpc_port", getPortMapping(config, PORT_RPC)))
            .addVariables(configValue("seeds", SEEDS_FORMAT_JOINER.join(getSeedNodes())))
            .build();
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.CASSANDRA_SERVER_RUN)
            .setCassandraServerRunTask(
                    CassandraServerRunTask.newBuilder()
                            //TODO(BenWhitehead) Cleanup path handling to make more maintainable across different versions of cassandra
                            .addAllCommand(newArrayList("apache-cassandra-" + config.getCassandraVersion() + "/bin/cassandra", "-p", "cassandra.pid"))
                            .setTaskConfig(taskConfig)
                            .setVersion(config.getCassandraVersion())
                            .setTaskEnv(taskEnv(
                                    // see conf/cassandra-env.sh in the cassandra distribution for details
                                    // about these variables.
                                    tuple2("JMX_PORT", String.valueOf(node.getJmxConnect().getJmxPort())),
                                    tuple2("MAX_HEAP_SIZE", config.getMemMb() + "m"),
                                    // The example HEAP_NEWSIZE assumes a modern 8-core+ machine for decent pause
                                    // times. If in doubt, and if you do not particularly want to tweak, go with
                                    // 100 MB per physical CPU core.
                                    tuple2("HEAP_NEWSIZE", (int) (config.getCpuCores() * 100) + "m")
                            ))
                            .setJmx(node.getJmxConnect())
            )
                .build();

        return CassandraNodeTask.newBuilder()
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .setCpuCores(configuration.cpuCores())
            .setMemMb(configuration.memMb())
            .setDiskMb(configuration.diskMb())
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

        return CassandraNodeExecutor.newBuilder()
            .setExecutorId(executorId)
            .setSource(configuration.frameworkName())
            .setCpuCores(0.1)
            .setMemMb(16)
            .setDiskMb(16)
            .setCommand(javaExec)
            .addCommandArgs("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
            .addCommandArgs("-XX:+PrintCommandLineFlags")
            .addCommandArgs("$JAVA_OPTS")
            .addCommandArgs("-classpath")
            .addCommandArgs("cassandra-executor.jar")
            .addCommandArgs("io.mesosphere.mesos.frameworks.cassandra.CassandraExecutor")
            .setTaskEnv(taskEnvFromMap(executorEnv))
            .addAllResource(newArrayList(
                    resourceUri(getUrlForResource("/jre-7-" + osName + ".tar.gz"), true),
                    resourceUri(getUrlForResource("/apache-cassandra-" + configuration.cassandraVersion() + "-bin.tar.gz"), true),
                    resourceUri(getUrlForResource("/cassandra-executor.jar"), false)
            ))
            .build();
    }

    @NotNull
    private static CassandraNodeTask getHealthCheckTask(@NotNull final String executorId, final String taskId) {
        return CassandraNodeTask.newBuilder()
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .setCpuCores(0.1)
            .setMemMb(16)
            .setDiskMb(16)
            .setTaskDetails(getHealthCheckTaskDetails())
            .build();
    }

    private static TaskDetails getHealthCheckTaskDetails() {
        return TaskDetails.newBuilder()
                .setTaskType(TaskDetails.TaskType.HEALTH_CHECK)
                .setHealthCheckTask(HealthCheckTask.getDefaultInstance())
                .build();
    }

    @NotNull
    private static CassandraNodeTask getMetadataTask(@NotNull final String executorId) {
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.EXECUTOR_METADATA)
            .setExecutorMetadataTask(
                ExecutorMetadataTask.newBuilder()
                    .setExecutorId(executorId)
            )
            .build();
        return CassandraNodeTask.newBuilder()
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

}
