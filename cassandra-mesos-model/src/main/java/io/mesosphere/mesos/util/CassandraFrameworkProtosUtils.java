package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import org.jetbrains.annotations.NotNull;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;

public final class CassandraFrameworkProtosUtils {

    public CassandraFrameworkProtosUtils() {}

    @NotNull
    @SafeVarargs
    public static TaskEnv taskEnv(@NotNull final Tuple2<String, String>... tuples) {
        return TaskEnv.newBuilder()
            .addAllVariables(from(newArrayList(tuples)).transform(tupleToTaskEnvEntry()))
            .build();
    }

    @NotNull
    public static Function<Tuple2<String, String>, TaskEnv.Entry> tupleToTaskEnvEntry() {
        return TupleToTaskEnvEntry.INSTANCE;
    }

    @NotNull
    public static Function<ExecutorMetadata, String> executorMetadataToIp() {
        return SlaveMetadataToIp.INSTANCE;
    }

    @NotNull
    public static Function<CassandraFrameworkProtos.CassandraNode, CassandraFrameworkProtos.CassandraNode.Builder> cassandraNodeToBuilder() {
        return CassandraNodeToBuilder.INSTANCE;
    }

    @NotNull
    public static Function<CassandraNode.Builder, CassandraNode> cassandraNodeBuilderToCassandraNode() {
        return CassandraNodeBuilderToCassandraNode.INSTANCE;
    }

    @NotNull
    public static Function<CassandraFrameworkProtos.ExecutorMetadata, CassandraFrameworkProtos.ExecutorMetadata.Builder> executorMetadataToBuilder() {
        return ExecutorMetadataToBuilder.INSTANCE;
    }

    @NotNull
    public static Function<ExecutorMetadata.Builder, ExecutorMetadata> executorMetadataBuilderToExecutorMetadata() {
        return ExecutorMetadataBuilderToExecutorMetadata.INSTANCE;
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeForTaskId(@NotNull final String taskId) {
        return Predicates.or(
            cassandraNodeMetadataTaskIdEq(taskId),
            cassandraNodeServerTaskIdEq(taskId)
        );
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeMetadataTaskIdEq(@NotNull final String taskId) {
        return new CassandraNodeMetadataTaskIdEq(taskId);
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeServerTaskIdEq(@NotNull final String taskId) {
        return new CassandraNodeServerTaskIdEq(taskId);
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeHasExecutor() {
        return new CassandraNodeHasExecutor();
    }

    @NotNull
    public static Function<CassandraNode, String> executorIdFromCassandraNode() {
        return ExecutorIdFromCassandraNode.INSTANCE;
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeHostnameEq(@NotNull final String hostname) {
        return new CassandraNodeHostnameEq(hostname);
    }

    @NotNull
    public static Predicate<ExecutorMetadata> executorMetadataExecutorIdEq(@NotNull final String executorId) {
        return new ExecutorMetadataExecutorIdEq(executorId);
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeHasServerTask() {
        return new CassandraNodeHasServerTask();
    }

    @NotNull
    public static Predicate<CassandraNode> cassandraNodeExecutorIdEq(@NotNull final String executorId) {
        return new CassandraNodeExecutorIdEq(executorId);
    }

    @NotNull
    public static Predicate<HealthCheckHistoryEntry> healthCheckHistoryEntryExecutorIdEq(@NotNull final String executorId) {
        return new HealthCheckHistoryEntryExecutorIdEq(executorId);
    }

    @NotNull
    public static Function<HealthCheckHistoryEntry, String> healthCheckHistoryEntryToExecutorId() {
        return HealthCheckHistoryEntryToExecutorId.INSTANCE;
    }

    @NotNull
    public static Function<HealthCheckHistoryEntry, Long> healthCheckHistoryEntryToTimestamp() {
        return HealthCheckHistoryEntryToTimestamp.INSTANCE;
    }

    @NotNull
    public static URI resourceUri(@NotNull final String urlForResource, final boolean extract) {
        return URI.newBuilder().setValue(urlForResource).setExtract(extract).build();
    }

    public static TaskConfig.Entry configValue(final String name, final Long value) {
        return TaskConfig.Entry.newBuilder().setName(name).setLongValue(value).build();
    }

    public static TaskConfig.Entry configValue(final String name, final String value) {
        return TaskConfig.Entry.newBuilder().setName(name).setStringValue(value).build();
    }

    private static final class TupleToTaskEnvEntry implements Function<Tuple2<String, String>, TaskEnv.Entry> {
        private static final TupleToTaskEnvEntry INSTANCE = new TupleToTaskEnvEntry();
        @Override
        public TaskEnv.Entry apply(final Tuple2<String, String> input) {
            return TaskEnv.Entry.newBuilder().setName(input._1).setValue(input._2).build();
        }
    }

    private static final class SlaveMetadataToIp implements Function<ExecutorMetadata, String> {
        private static final SlaveMetadataToIp INSTANCE = new SlaveMetadataToIp();

        @Override
        public String apply(final ExecutorMetadata input) {
            return input.getIp();
        }
    }

    private static final class CassandraNodeToBuilder implements Function<CassandraFrameworkProtos.CassandraNode, CassandraFrameworkProtos.CassandraNode.Builder> {
        private static final CassandraNodeToBuilder INSTANCE = new CassandraNodeToBuilder();

        @Override
        public CassandraNode.Builder apply(final CassandraNode input) {
            return CassandraNode.newBuilder(input);
        }
    }

    private static final class CassandraNodeBuilderToCassandraNode implements Function<CassandraNode.Builder, CassandraNode> {
        private static final CassandraNodeBuilderToCassandraNode INSTANCE = new CassandraNodeBuilderToCassandraNode();

        @Override
        public CassandraNode apply(final CassandraNode.Builder input) {
            return input.build();
        }
    }

    private static final class ExecutorMetadataToBuilder implements Function<CassandraFrameworkProtos.ExecutorMetadata, CassandraFrameworkProtos.ExecutorMetadata.Builder> {
        private static final ExecutorMetadataToBuilder INSTANCE = new ExecutorMetadataToBuilder();

        @Override
        public ExecutorMetadata.Builder apply(final ExecutorMetadata input) {
            return ExecutorMetadata.newBuilder(input);
        }
    }

    private static final class ExecutorMetadataBuilderToExecutorMetadata implements Function<ExecutorMetadata.Builder, ExecutorMetadata> {
        private static final ExecutorMetadataBuilderToExecutorMetadata INSTANCE = new ExecutorMetadataBuilderToExecutorMetadata();

        @Override
        public ExecutorMetadata apply(final ExecutorMetadata.Builder input) {
            return input.build();
        }
    }

    private static final class CassandraNodeMetadataTaskIdEq implements Predicate<CassandraNode> {
        @NotNull
        private final String metadataTaskId;

        public CassandraNodeMetadataTaskIdEq(@NotNull final String metadataTaskId) {
            this.metadataTaskId = metadataTaskId;
        }

        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasMetadataTask() && item.getMetadataTask().getTaskId().equals(metadataTaskId);
        }
    }

    private static final class CassandraNodeServerTaskIdEq implements Predicate<CassandraNode> {
        @NotNull
        private final String taskId;

        public CassandraNodeServerTaskIdEq(@NotNull final String taskId) {
            this.taskId = taskId;
        }

        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasServerTask() && item.getServerTask().getTaskId().equals(taskId);
        }
    }

    private static final class ExecutorIdFromCassandraNode implements Function<CassandraNode, String> {
        private static final ExecutorIdFromCassandraNode INSTANCE = new ExecutorIdFromCassandraNode();

        @Override
        public String apply(final CassandraNode input) {
            return input.getCassandraNodeExecutor().getExecutorId();
        }
    }

    private static final class CassandraNodeHasExecutor implements Predicate<CassandraNode> {
        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasCassandraNodeExecutor();
        }
    }

    private static final class CassandraNodeHostnameEq implements Predicate<CassandraNode> {
        @NotNull
        private final String hostname;

        public CassandraNodeHostnameEq(@NotNull final String hostname) {
            this.hostname = hostname;
        }

        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasHostname() && item.getHostname().equals(hostname);
        }
    }

    private static final class ExecutorMetadataExecutorIdEq implements Predicate<ExecutorMetadata> {
        @NotNull
        private final String executorId;

        public ExecutorMetadataExecutorIdEq(@NotNull final String executorId) {
            this.executorId = executorId;
        }

        @Override
        public boolean apply(final ExecutorMetadata item) {
            return item.getExecutorId().equals(executorId);
        }
    }


    private static final class CassandraNodeHasServerTask implements Predicate<CassandraNode> {
        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasServerTask();
        }
    }

    private static final class CassandraNodeExecutorIdEq implements Predicate<CassandraNode> {
        @NotNull
        private final String executorId;

        public CassandraNodeExecutorIdEq(@NotNull final String executorId) {
            this.executorId = executorId;
        }

        @Override
        public boolean apply(final CassandraNode item) {
            return item.hasCassandraNodeExecutor() && item.getCassandraNodeExecutor().getExecutorId().equals(executorId);
        }
    }

    private static final class HealthCheckHistoryEntryExecutorIdEq implements Predicate<HealthCheckHistoryEntry> {
        @NotNull
        private final String executorId;

        public HealthCheckHistoryEntryExecutorIdEq(@NotNull final String executorId) {
            this.executorId = executorId;
        }

        @Override
        public boolean apply(final HealthCheckHistoryEntry item) {
            return item.getExecutorId().equals(executorId);
        }
    }

    private static final class HealthCheckHistoryEntryToExecutorId implements Function<HealthCheckHistoryEntry, String> {
        private static final HealthCheckHistoryEntryToExecutorId INSTANCE = new HealthCheckHistoryEntryToExecutorId();

        @Override
        public String apply(final HealthCheckHistoryEntry input) {
            return input.getExecutorId();
        }
    }

    private static final class HealthCheckHistoryEntryToTimestamp implements Function<HealthCheckHistoryEntry, Long> {
        private static final HealthCheckHistoryEntryToTimestamp INSTANCE = new HealthCheckHistoryEntryToTimestamp();

        @Override
        public Long apply(final HealthCheckHistoryEntry input) {
            return input.getTimestamp();
        }
    }

    private static final class CassandraNodeToExecutor implements Function<CassandraNode, CassandraNodeExecutor> {
        private static final CassandraNodeToExecutor INSTANCE = new CassandraNodeToExecutor();

        @Override
        public CassandraNodeExecutor apply(final CassandraNode input) {
            return input.getCassandraNodeExecutor();
        }
    }

    @NotNull
    public static Function<CassandraNode, CassandraNodeExecutor> cassandraNodeToExecutor() {
        return CassandraNodeToExecutor.INSTANCE;
    }


}
