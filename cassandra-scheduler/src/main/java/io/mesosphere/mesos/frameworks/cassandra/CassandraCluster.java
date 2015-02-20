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
import io.mesosphere.mesos.util.Tuple2;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.CassandraFrameworkProtosUtils.*;
import static io.mesosphere.mesos.util.Functions.*;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;
import static java.util.Collections.unmodifiableList;

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

    // see: http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
    private static final Map<String, Long> defaultCassandraPortMappings = unmodifiableHashMap(
        tuple2("storage_port", 7000L),
        tuple2("ssl_storage_port", 7001L),
        tuple2("jmx_port", 7199L),
        tuple2("native_transport_port", 9042L),
        tuple2("rpc_port", 9160L)
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

    public CassandraCluster(
        @NotNull final Clock clock,
        @NotNull final String httpServerBaseUrl,
        @NotNull final ExecutorCounter execCounter,
        @NotNull final PersistedCassandraClusterState clusterState,
        @NotNull final PersistedCassandraClusterHealthCheckHistory healthCheckHistory,
        @NotNull final PersistedCassandraFrameworkConfiguration configuration
    ) {
        this.clock = clock;
        this.httpServerBaseUrl = httpServerBaseUrl;
        this.execCounter = execCounter;
        this.clusterState = clusterState;
        this.healthCheckHistory = healthCheckHistory;
        this.configuration = configuration;
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

    public void recordHealthCheck(@NotNull final String executorId, @NotNull final CassandraNodeHealthCheckDetails details) {
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
                removeTask(nodeOpt.get().getServerTask().getTaskId());
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

    @NotNull
    public Optional<Tuple2<CassandraNodeExecutor, List<CassandraNodeTask>>> getTasksForOffer(@NotNull final Protos.Offer offer) {
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        LOGGER.debug(marker, "> getTasksForOffer(offer : {})", protoToString(offer));

        final Optional<CassandraNode> nodeOption = headOption(
            from(clusterState.nodes())
                .filter(cassandraNodeHostnameEq(offer.getHostname()))
        );

        if (nodeOption.isPresent()) {
            final CassandraNode.Builder node = CassandraNode.newBuilder(nodeOption.get());

            final List<CassandraNodeTask> cassandraNodeTasks = newArrayList();
            if (!node.hasCassandraNodeExecutor()) {
                final String executorId = getExecutorIdForOffer(offer);
                final CassandraNodeExecutor executor = getCassandraNodeExecutorSupplier(executorId);
                node.setCassandraNodeExecutor(executor);
            }

            final CassandraNodeExecutor executor = node.getCassandraNodeExecutor();
            final String executorId = executor.getExecutorId();
            if (!node.hasMetadataTask()) {
                final CassandraNodeTask metadataTask = getMetadataTask(executorId);
                node.setMetadataTask(metadataTask);
                cassandraNodeTasks.add(metadataTask);
            } else if (!node.hasServerTask()) {
                final List<String> errors = hasResources(
                    offer,
                    configuration.cpuCores(),
                    configuration.memMb(),
                    configuration.diskMb(),
                    defaultCassandraPortMappings
                );
                if (!errors.isEmpty()) {
                    LOGGER.info(marker, "Insufficient resources in offer: {}. Details: ['{}']", offer.getId().getValue(), JOINER.join(errors));
                } else {
                    final String taskId = executorId + ".server";
                    final Optional<ExecutorMetadata> maybeMetadata = getExecutorMetadata(executorId);
                    if (maybeMetadata.isPresent()) {
                        final ExecutorMetadata metadata = maybeMetadata.get();
                        final CassandraNodeTask task = getServerTask(executorId, taskId, metadata, node.getJmxConnect());
                        node.setServerTask(task);
                        cassandraNodeTasks.add(task);
                    }
                }
            } else if (shouldRunHealthCheck(executorId)) {
                final String taskId = node.getCassandraNodeExecutor().getExecutorId() + ".healthcheck";
                final CassandraNodeTask task = getHealthCheckTask(executorId, taskId);
                cassandraNodeTasks.add(task);
            }
            final List<CassandraNodeTask> tasks = unmodifiableList(cassandraNodeTasks);
            LOGGER.trace(marker, "< getTasksForOffer(offer : {}) = {}", protoToString(offer), protoToString(tasks));
            final CassandraNode built = node.build();
            clusterState.addOrSetNode(built);
            final Tuple2<CassandraNodeExecutor, List<CassandraNodeTask>> retVal = tuple2(built.getCassandraNodeExecutor(), tasks);
            return Optional.of(retVal);
        } else if (launchNode()) {
            clusterState.nodes(append(
                    clusterState.nodes(),
                    buildCassandraNode(offer)
            ));
            return Optional.absent();
        } else {
            return Optional.absent();
        }
    }

    private static CassandraNode buildCassandraNode(Protos.Offer offer) {
        CassandraNode.Builder builder = CassandraNode.newBuilder()
                .setHostname(offer.getHostname());
        try {
            InetAddress ia = InetAddress.getByName(offer.getHostname());

            int jmxPort = defaultCassandraPortMappings.get("jmx_port").intValue();
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
            @NotNull final JmxConnect jmxConnect) {
        final TaskConfig taskConfig = TaskConfig.newBuilder()
            .addVariables(configValue("cluster_name", configuration.frameworkName()))
            .addVariables(configValue("broadcast_address", metadata.getIp()))
            .addVariables(configValue("rpc_address", metadata.getIp()))
            .addVariables(configValue("listen_address", metadata.getIp()))
            .addVariables(configValue("storage_port", defaultCassandraPortMappings.get("storage_port")))
            .addVariables(configValue("ssl_storage_port", defaultCassandraPortMappings.get("ssl_storage_port")))
            .addVariables(configValue("native_transport_port", defaultCassandraPortMappings.get("native_transport_port")))
            .addVariables(configValue("rpc_port", defaultCassandraPortMappings.get("rpc_port")))
            .addVariables(configValue("seeds", SEEDS_FORMAT_JOINER.join(getSeedNodes())))
            .build();
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.CASSANDRA_SERVER_RUN)
            .setCassandraServerRunTask(
                    CassandraServerRunTask.newBuilder()
                            //TODO(BenWhitehead) Cleanup path handling to make more maintainable across different versions of cassandra
                            .addAllCommand(newArrayList("apache-cassandra-" + configuration.cassandraVersion() + "/bin/cassandra", "-p", "cassandra.pid"))
                            .setTaskConfig(taskConfig)
                            .setVersion(configuration.cassandraVersion())
                            .setTaskEnv(taskEnv(
                                    // see conf/cassandra-env.sh in the cassandra distribution for details
                                    // about these variables.
                                    tuple2("JMX_PORT", String.valueOf(defaultCassandraPortMappings.get("jmx_port"))),
                                    tuple2("MAX_HEAP_SIZE", configuration.memMb() + "m"),
                                    // The example HEAP_NEWSIZE assumes a modern 8-core+ machine for decent pause
                                    // times. If in doubt, and if you do not particularly want to tweak, go with
                                    // 100 MB per physical CPU core.
                                    tuple2("HEAP_NEWSIZE", (int) (configuration.cpuCores() * 100) + "m")
                            ))
                            .setJmx(jmxConnect)
            )
            .build();

        return CassandraNodeTask.newBuilder()
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .setCpuCores(configuration.cpuCores())
            .setMemMb(configuration.memMb())
            .setDiskMb(configuration.diskMb())
            .addAllPorts(defaultCassandraPortMappings.values())
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
//            .addCommandArgs("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
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
    private static CassandraNodeTask getHealthCheckTask(@NotNull final String executorId, @NotNull final String taskId) {
        final TaskDetails taskDetails = TaskDetails.newBuilder()
            .setTaskType(TaskDetails.TaskType.CASSANDRA_SERVER_HEALTH_CHECK)
            .setCassandraServerHealthCheckTask(CassandraServerHealthCheckTask.getDefaultInstance())
            .build();
        return CassandraNodeTask.newBuilder()
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .setCpuCores(0.1)
            .setMemMb(16)
            .setDiskMb(16)
            .setTaskDetails(taskDetails)
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
