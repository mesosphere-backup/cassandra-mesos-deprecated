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
package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;

public final class CassandraFrameworkProtosUtils {

    private CassandraFrameworkProtosUtils() {}

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
    public static Function<CassandraNode, String> cassandraNodeToIp() {
        return CassandraNodeToIp.INSTANCE;
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

    public static TaskConfig.Entry configValue(@NotNull final String name, @NotNull final Integer value) {
        return TaskConfig.Entry.newBuilder().setName(name).setLongValue(value).build();
    }

    public static TaskConfig.Entry configValue(@NotNull final String name, @NotNull final String value) {
        return TaskConfig.Entry.newBuilder().setName(name).setStringValue(value).build();
    }

    public static List<String> getSeedNodeIps(@NotNull List<CassandraNode> nodes) {
        return newArrayList(from(nodes)
            .filter(new Predicate<CassandraNode>() {
                @Override
                public boolean apply(CassandraNode v) {
                    return v.getSeed();
                }
            })
            .transform(cassandraNodeToIp()));
    }

    private static final class TupleToTaskEnvEntry implements Function<Tuple2<String, String>, TaskEnv.Entry> {
        private static final TupleToTaskEnvEntry INSTANCE = new TupleToTaskEnvEntry();
        @Override
        public TaskEnv.Entry apply(@NotNull final Tuple2<String, String> input) {
            return TaskEnv.Entry.newBuilder().setName(input._1).setValue(input._2).build();
        }
    }

    private static final class SlaveMetadataToIp implements Function<ExecutorMetadata, String> {
        private static final SlaveMetadataToIp INSTANCE = new SlaveMetadataToIp();

        @Override
        public String apply(@NotNull final ExecutorMetadata input) {
            return input.getIp();
        }
    }

    private static final class CassandraNodeToIp implements Function<CassandraNode, String> {
        private static final CassandraNodeToIp INSTANCE = new CassandraNodeToIp();

        @Override
        public String apply(@NotNull final CassandraNode input) {
            return input.getIp();
        }
    }

    private static final class CassandraNodeToBuilder implements Function<CassandraFrameworkProtos.CassandraNode, CassandraFrameworkProtos.CassandraNode.Builder> {
        private static final CassandraNodeToBuilder INSTANCE = new CassandraNodeToBuilder();

        @Override
        public CassandraNode.Builder apply(@NotNull final CassandraNode input) {
            return CassandraNode.newBuilder(input);
        }
    }

    private static final class CassandraNodeBuilderToCassandraNode implements Function<CassandraNode.Builder, CassandraNode> {
        private static final CassandraNodeBuilderToCassandraNode INSTANCE = new CassandraNodeBuilderToCassandraNode();

        @Override
        public CassandraNode apply(@NotNull final CassandraNode.Builder input) {
            return input.build();
        }
    }

    private static final class ExecutorMetadataToBuilder implements Function<CassandraFrameworkProtos.ExecutorMetadata, CassandraFrameworkProtos.ExecutorMetadata.Builder> {
        private static final ExecutorMetadataToBuilder INSTANCE = new ExecutorMetadataToBuilder();

        @Override
        public ExecutorMetadata.Builder apply(@NotNull final ExecutorMetadata input) {
            return ExecutorMetadata.newBuilder(input);
        }
    }

    private static final class ExecutorMetadataBuilderToExecutorMetadata implements Function<ExecutorMetadata.Builder, ExecutorMetadata> {
        private static final ExecutorMetadataBuilderToExecutorMetadata INSTANCE = new ExecutorMetadataBuilderToExecutorMetadata();

        @Override
        public ExecutorMetadata apply(@NotNull final ExecutorMetadata.Builder input) {
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
        public boolean apply(@NotNull final CassandraNode item) {
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
        public boolean apply(@NotNull final CassandraNode item) {
            return item.hasServerTask() && item.getServerTask().getTaskId().equals(taskId);
        }
    }

    private static final class ExecutorIdFromCassandraNode implements Function<CassandraNode, String> {
        private static final ExecutorIdFromCassandraNode INSTANCE = new ExecutorIdFromCassandraNode();

        @Override
        public String apply(@NotNull final CassandraNode input) {
            return input.getCassandraNodeExecutor().getExecutorId();
        }
    }

    private static final class CassandraNodeHasExecutor implements Predicate<CassandraNode> {
        @Override
        public boolean apply(@NotNull final CassandraNode item) {
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
        public boolean apply(@NotNull final CassandraNode item) {
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
        public boolean apply(@NotNull final ExecutorMetadata item) {
            return item.getExecutorId().equals(executorId);
        }
    }


    private static final class CassandraNodeHasServerTask implements Predicate<CassandraNode> {
        @Override
        public boolean apply(@NotNull final CassandraNode item) {
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
        public boolean apply(@NotNull final CassandraNode item) {
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
        public boolean apply(@NotNull final HealthCheckHistoryEntry item) {
            return item.getExecutorId().equals(executorId);
        }
    }

    private static final class HealthCheckHistoryEntryToExecutorId implements Function<HealthCheckHistoryEntry, String> {
        private static final HealthCheckHistoryEntryToExecutorId INSTANCE = new HealthCheckHistoryEntryToExecutorId();

        @Override
        public String apply(@NotNull final HealthCheckHistoryEntry input) {
            return input.getExecutorId();
        }
    }

    private static final class HealthCheckHistoryEntryToTimestamp implements Function<HealthCheckHistoryEntry, Long> {
        private static final HealthCheckHistoryEntryToTimestamp INSTANCE = new HealthCheckHistoryEntryToTimestamp();

        @Override
        public Long apply(@NotNull final HealthCheckHistoryEntry input) {
            return input.getTimestampLast();
        }
    }

    private static final class CassandraNodeToExecutor implements Function<CassandraNode, CassandraNodeExecutor> {
        private static final CassandraNodeToExecutor INSTANCE = new CassandraNodeToExecutor();

        @Override
        public CassandraNodeExecutor apply(@NotNull final CassandraNode input) {
            return input.getCassandraNodeExecutor();
        }
    }

    @NotNull
    public static Function<CassandraNode, CassandraNodeExecutor> cassandraNodeToExecutor() {
        return CassandraNodeToExecutor.INSTANCE;
    }


}
