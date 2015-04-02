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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public final class PersistedCassandraClusterHealthCheckHistory extends StatePersistedObject<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory> {
    static final int DEFAULT_MAX_ENTRIES_PER_NODE = 5;

    public PersistedCassandraClusterHealthCheckHistory(
        @NotNull final State state
    ) {
        super(
            "CassandraClusterHealthCheckHistory",
            state,
            new Supplier<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterHealthCheckHistory get() {
                    return CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.newBuilder()
                        .setMaxEntriesPerNode(DEFAULT_MAX_ENTRIES_PER_NODE)
                        .build();
                }
            },
            new Function<byte[], CassandraFrameworkProtos.CassandraClusterHealthCheckHistory>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterHealthCheckHistory apply(final byte[] input) {
                    try {
                        return CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.parseFrom(input);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ProtoUtils.RuntimeInvalidProtocolBufferException(e);
                    }
                }
            },
            new Function<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory, byte[]>() {
                @Override
                public byte[] apply(final CassandraFrameworkProtos.CassandraClusterHealthCheckHistory input) {
                    return input.toByteArray();
                }
            }
        );
    }

    @NotNull
    public List<CassandraFrameworkProtos.HealthCheckHistoryEntry> entries() {
        return get().getEntriesList();
    }

    @NotNull
    public List<CassandraFrameworkProtos.HealthCheckHistoryEntry> entriesForExecutor(String executorId) {
        CassandraFrameworkProtos.CassandraClusterHealthCheckHistory history = get();
        List<CassandraFrameworkProtos.HealthCheckHistoryEntry> forNode = new ArrayList<>(history.getMaxEntriesPerNode());
        int count = history.getEntriesCount();
        for (int i = 0; i < count; i++) {
            CassandraFrameworkProtos.HealthCheckHistoryEntry hc = history.getEntries(i);
            if (executorId.equals(hc.getExecutorId()))
                forNode.add(hc);
        }
        return forNode;
    }

    public CassandraFrameworkProtos.HealthCheckHistoryEntry last(String executorId) {
        CassandraFrameworkProtos.CassandraClusterHealthCheckHistory history = get();
        int count = history.getEntriesCount();
        for (int i = count - 1; i >= 0; i--) {
            CassandraFrameworkProtos.HealthCheckHistoryEntry hc = history.getEntries(i);
            if (executorId.equals(hc.getExecutorId()))
                return hc;
        }
        return null;
    }

    /**
     * The implementation does not add a new entry when it is similar to the previous one.
     * Instead it updates the timespan in the previous one.
     */
    public void record(String executorId, long timestamp, @NotNull final CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
        CassandraFrameworkProtos.CassandraClusterHealthCheckHistory prev = get();
        int maxEntriesPerNode = prev.getMaxEntriesPerNode();
        CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.Builder builder =
            CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(maxEntriesPerNode);

        // copy entries from other nodes to new value, collect old entries from current node in temporary list
        List<CassandraFrameworkProtos.HealthCheckHistoryEntry> forNode = new ArrayList<>(maxEntriesPerNode);
        for (CassandraFrameworkProtos.HealthCheckHistoryEntry healthCheckHistoryEntry : prev.getEntriesList()) {
            if (healthCheckHistoryEntry.getExecutorId().equals(executorId)) {
                forNode.add(healthCheckHistoryEntry);
            } else {
                builder.addEntries(healthCheckHistoryEntry);
            }
        }

        if (forNode.isEmpty()) {
            // first history entry, just add it to the builder
            builder.addEntries(buildEntry(executorId, timestamp, healthCheckDetails));
        } else {
            // Check if previous entry is similar to the previous.
            // If yes, then just update HealthCheckHistoryEntry.timestampLast,
            // otherwise add the entry and remove the eldest historic entry.
            CassandraFrameworkProtos.HealthCheckHistoryEntry last = forNode.get(forNode.size() - 1);
            if (last.getTimestampEnd() > timestamp) {
                // we already have more recent information - discard the current details
                return;
            }
            if (isSimilarEntry(last.getDetails(), healthCheckDetails)) {
                for (int i = 0; i < forNode.size() - 1; i++) {
                    builder.addEntries(forNode.get(i));
                }
                builder.addEntries(buildEntry(executorId, timestamp, healthCheckDetails)
                    .setTimestampStart(last.getTimestampStart()));
            } else {
                for (int i = Math.max(0, forNode.size() + 1 - maxEntriesPerNode); i < forNode.size(); i++)
                    builder.addEntries(forNode.get(i));
                builder.addEntries(buildEntry(executorId, timestamp, healthCheckDetails));
            }
        }

        setValue(builder.build());
    }

    static boolean isSimilarEntry(CassandraFrameworkProtos.HealthCheckDetails existing, CassandraFrameworkProtos.HealthCheckDetails current) {
        for (Descriptors.FieldDescriptor f : existing.getDescriptorForType().getFields()) {
            if (!"info".equals(f.getName())) {
                if (!objEquals(existing.getField(f), current.getField(f))) {
                    return false;
                }
            } else {
                if (!isSimilarEntry(existing.getInfo(), current.getInfo())) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean isSimilarEntry(CassandraFrameworkProtos.NodeInfo existing, CassandraFrameworkProtos.NodeInfo current) {
        for (Descriptors.FieldDescriptor f : existing.getDescriptorForType().getFields()) {
            // ignore 'uptime' field
            if (!"uptimeMillis".equals(f.getName())) {
                if (!objEquals(existing.getField(f), current.getField(f))) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean objEquals(Object o1, Object o2) {
        return o1 == null && o2 == null || !(o1 == null || o2 == null) && o1.equals(o2);
    }

    private static CassandraFrameworkProtos.HealthCheckHistoryEntry.Builder buildEntry(String executorId, long timestamp,
           CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
        return CassandraFrameworkProtos.HealthCheckHistoryEntry.newBuilder()
            .setExecutorId(executorId)
            .setTimestampStart(timestamp)
            .setTimestampEnd(timestamp)
            .setDetails(healthCheckDetails);
    }
}
