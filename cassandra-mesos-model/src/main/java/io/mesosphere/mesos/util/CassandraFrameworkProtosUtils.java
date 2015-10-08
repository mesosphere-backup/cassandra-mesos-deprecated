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
package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.ProtoUtils.*;

public final class CassandraFrameworkProtosUtils {

    private CassandraFrameworkProtosUtils() {}

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
    public static Predicate<CassandraNode> cassandraNodeForTaskId(@NotNull final String taskId) {
        return new CassandraNodeTaskIdEq(taskId);
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
    public static Predicate<CassandraNode> cassandraNodeExecutorIdEq(@NotNull final String executorId) {
        return new CassandraNodeExecutorIdEq(executorId);
    }

    @NotNull
    public static Predicate<HealthCheckHistoryEntry> healthCheckHistoryEntryExecutorIdEq(@NotNull final String executorId) {
        return new HealthCheckHistoryEntryExecutorIdEq(executorId);
    }

    @NotNull
    public static Predicate<Resource> resourceHasExpectedRole(@NotNull final String role) {
        return new ResourceHasExpectedRole(role);
    }

    @NotNull
    public static Function<HealthCheckHistoryEntry, Long> healthCheckHistoryEntryToTimestamp() {
        return HealthCheckHistoryEntryToTimestamp.INSTANCE;
    }

    @NotNull
    public static FileDownload resourceFileDownload(@NotNull final String urlForResource, final boolean extract) {
        return FileDownload.newBuilder().setDownloadUrl(urlForResource).setExtract(extract).build();
    }

    public static TaskConfig.Entry configValue(@NotNull final String name, @NotNull final Integer value) {
        return TaskConfig.Entry.newBuilder().setName(name).setLongValue(value).build();
    }

    public static TaskConfig.Entry configValue(@NotNull final String name, @NotNull final String value) {
        return TaskConfig.Entry.newBuilder().setName(name).setStringValue(value).build();
    }

    @NotNull
    public static TaskConfig.Entry configValue(@NotNull final String name, @NotNull final List<String> values) {
        return TaskConfig.Entry.newBuilder().setName(name).addAllStringValues(values).build();
    }

    public static List<String> getSeedNodeIps(@NotNull final List<CassandraNode> nodes) {
        return newArrayList(from(nodes)
            .filter(new Predicate<CassandraNode>() {
                @Override
                public boolean apply(final CassandraNode v) {
                    return v.getSeed();
                }
            })
            .transform(cassandraNodeToIp()));
    }

    public static void addTaskEnvEntry(@NotNull final TaskEnv.Builder taskEnv, final boolean replace, @NotNull final String name, @NotNull final String value) {
        for (int i = 0; i < taskEnv.getVariablesList().size(); i++) {
            final TaskEnv.Entry entry = taskEnv.getVariables(i);
            if (name.equals(entry.getName())) {
                if (replace)
                    taskEnv.setVariables(i, TaskEnv.Entry.newBuilder().setName(name).setValue(value).build());
                return;
            }
        }
        taskEnv.addVariables(TaskEnv.Entry.newBuilder().setName(name).setValue(value).build());
    }

    public static void setTaskConfig(@NotNull final TaskConfig.Builder taskConfig, @NotNull final TaskConfig.Entry entry) {
        for (int i = 0; i < taskConfig.getVariablesList().size(); i++) {
            final TaskConfig.Entry e = taskConfig.getVariables(i);
            if (entry.getName().equals(e.getName())) {
                taskConfig.setVariables(i, entry);
                return;
            }
        }
        taskConfig.addVariables(entry);
    }

    public static CassandraNodeTask getTaskForNode(@NotNull final CassandraNode cassandraNode, @NotNull final String taskId) {
        for (final CassandraNodeTask cassandraNodeTask : cassandraNode.getTasksList()) {
            if (cassandraNodeTask.getTaskId().equals(taskId))
                return cassandraNodeTask;
        }
        return null;
    }

    public static CassandraNodeTask getTaskForNode(@NotNull final CassandraNode cassandraNode, @NotNull final CassandraNodeTask.NodeTaskType taskType) {
        for (final CassandraNodeTask cassandraNodeTask : cassandraNode.getTasksList()) {
            if (cassandraNodeTask.getType() == taskType)
                return cassandraNodeTask;
        }
        return null;
    }

    public static CassandraNode.Builder removeTask(@NotNull final CassandraNode cassandraNode, @NotNull final CassandraNodeTask nodeTask) {
        final CassandraNode.Builder builder = CassandraNode.newBuilder(cassandraNode);
        for (int i = 0; i < builder.getTasksList().size(); i++) {
            final CassandraNodeTask t = builder.getTasks(i);
            if (t.getTaskId().equals(nodeTask.getTaskId())) {
                builder.removeTasks(i);
                break;
            }
        }
        return builder;
    }

    @NotNull
    public static Function<Resource, TreeSet<Long>> resourceToPortSet() {
        return ResourceToPortSet.INSTANCE;
    }

    @NotNull
    public static Predicate<Resource> containsPort(long port) {
        return new ContainsPort(port);
    }

    public static ImmutableListMultimap<String, Resource> resourcesForRoleAndOffer(@NotNull final String role, @NotNull final Protos.Offer offer) {
        return from(offer.getResourcesList())
                .filter(resourceHasExpectedRole(role))
                .index(resourceToName());
    }

    public static ImmutableListMultimap<String, Resource> nonReservedResources(@NotNull final Protos.Offer offer) {
        return from(offer.getResourcesList())
            .filter(resourceHasExpectedRole("*"))
            .index(resourceToName());
    }

    public static Predicate<Resource> scalarValueAtLeast(final long v) {
        return new ScalarValueAtLeast(v);
    }

    public static Function<Resource, Double> toDoubleResourceValue() {
        return ToDoubleResourceValue.INSTANCE;
    }

    public static Function<Resource, Long> toLongResourceValue() {
        return ToLongResourceValue.INSTANCE;
    }

    public static Optional<Double> maxResourceValueDouble(@NotNull final List<Resource> resource) {
        ImmutableList<Double> values = from(resource)
                .transform(toDoubleResourceValue())
                .toList();
        if (values.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(Collections.max(values));
        }
    }

    public static Optional<Long> maxResourceValueLong(@NotNull final List<Resource> resource) {
        ImmutableList<Long> values = from(resource)
                .transform(toLongResourceValue())
                .toList();
        if (values.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(Collections.max(values));
        }
    }

    public static String roleForPort(final long port, @NotNull final ListMultimap<String, Resource> index) {
        Optional<Resource> offeredPorts = from(index.get("ports"))
                .filter(containsPort(port))
                .first();

        if (offeredPorts.isPresent()) {
            return offeredPorts.get().getRole();
        } else {
            return "*";
        }
    }

    public static String attributeValue(@NotNull final Protos.Offer offer, @NotNull final String name) {
        for (final Protos.Attribute attribute : offer.getAttributesList()) {
            if (name.equals(attribute.getName())) {
                return attribute.hasText() ? attribute.getText().getValue() : null;
            }
        }

        return null;
    }

    public static Function<Map.Entry<String, Collection<Long>>, Resource> roleAndPortsToResource() {
        return RoleAndPortsToResource.INSTANCE;
    }

    public static Function<Long, String> byRole(@NotNull final ListMultimap<String, Resource> resourcesForRoleAndOffer) {
        return new ByRole(resourcesForRoleAndOffer);
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

    private static final class CassandraNodeTaskIdEq implements Predicate<CassandraNode> {
        @NotNull
        private final String taskId;

        public CassandraNodeTaskIdEq(@NotNull final String taskId) {
            this.taskId = taskId;
        }

        @Override
        public boolean apply(@NotNull final CassandraNode item) {
            for (final CassandraNodeTask cassandraNodeTask : item.getTasksList()) {
                if (cassandraNodeTask.getTaskId().equals(taskId))
                    return true;
            }
            return false;
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

    private static final class ResourceHasExpectedRole implements Predicate<Resource> {
        @NotNull
        private final String role;

        public ResourceHasExpectedRole(@NotNull final String role) {
            this.role = role;
        }

        @Override
        public boolean apply(final Resource item) {
            String givenRole = item.getRole();
            return givenRole.equals(role) || givenRole.equals("*");
        }
    }

    private static final class HealthCheckHistoryEntryToTimestamp implements Function<HealthCheckHistoryEntry, Long> {
        private static final HealthCheckHistoryEntryToTimestamp INSTANCE = new HealthCheckHistoryEntryToTimestamp();

        @Override
        public Long apply(@NotNull final HealthCheckHistoryEntry input) {
            return input.getTimestampEnd();
        }
    }

    private static class ResourceToPortSet implements Function<Resource, TreeSet<Long>> {
        private static final ResourceToPortSet INSTANCE = new ResourceToPortSet();

        @Override
        @NotNull
        public TreeSet<Long> apply(@Nullable final Resource resource) {
            return resourceValueRange(Optional.fromNullable(resource));
        }
    }

    private static class ContainsPort implements Predicate<Resource> {
        private final long port;

        public ContainsPort(final long port) {
            this.port = port;
        }

        @Override
        public boolean apply(@Nullable final Resource resource) {
            TreeSet<Long> portsInResource = resourceValueRange(Optional.fromNullable(resource));
            return portsInResource.contains(port);
        }
    }

    private static class ScalarValueAtLeast implements Predicate<Resource> {
        private final long v;

        public ScalarValueAtLeast(final long v) {
            this.v = v;
        }

        @Override
        public boolean apply(@NotNull final Resource resource) {
            return resource.getType() == Protos.Value.Type.SCALAR &&
                    resource.getScalar().getValue() > v;
        }
    }

    private static class ToDoubleResourceValue implements Function<Resource, Double> {
        private static final ToDoubleResourceValue INSTANCE = new ToDoubleResourceValue();

        @Override
        public Double apply(@Nullable final Resource resource) {
            return resourceValueDouble(Optional.fromNullable(resource)).or(0.0);
        }
    }

    private static class ToLongResourceValue implements Function<Resource, Long> {
        private static final ToLongResourceValue INSTANCE = new ToLongResourceValue();

        @Override
        public Long apply(@Nullable final Resource resource) {
            return resourceValueLong(Optional.fromNullable(resource)).or(0l);
        }
    }

    private static class RoleAndPortsToResource implements Function<Map.Entry<String,Collection<Long>>, Resource> {
        public static final RoleAndPortsToResource INSTANCE = new RoleAndPortsToResource();

        @Override
        public Resource apply(@NotNull final Map.Entry<String, Collection<Long>> roleAndPorts) {
            return ports(from(roleAndPorts.getValue()), roleAndPorts.getKey());
        }
    }

    private static class ByRole implements Function<Long, String> {
        private final ListMultimap<String, Resource> resourcesForRoleAndOffer;

        public ByRole(@NotNull final ListMultimap<String, Resource> resourcesForRoleAndOffer) {
            this.resourcesForRoleAndOffer = resourcesForRoleAndOffer;
        }

        @Override
        public String apply(@NotNull final Long port) {
            return roleForPort(port, resourcesForRoleAndOffer);
        }
    }
}
