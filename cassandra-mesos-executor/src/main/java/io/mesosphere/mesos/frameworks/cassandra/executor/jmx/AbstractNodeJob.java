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
package io.mesosphere.mesos.frameworks.cassandra.executor.jmx;

import com.google.common.base.Joiner;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public abstract class AbstractNodeJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeJob.class);

    private static final Joiner JOINER = Joiner.on("},{");

    protected final long startTimestamp = System.currentTimeMillis();

    @NotNull
    private final Protos.TaskID taskId;

    @Nullable
    protected JmxConnect jmxConnect;
    @Nullable
    private List<String> remainingKeyspaces;
    @NotNull
    private final Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> keyspaceStatus = new HashMap<>();

    private volatile long keyspaceStartedAt;

    private long finishedTimestamp;

    protected AbstractNodeJob(@NotNull final Protos.TaskID taskId) {
        this.taskId = taskId;
    }

    @NotNull
    public Protos.TaskID getTaskId() {
        return taskId;
    }

    @NotNull
    public abstract CassandraFrameworkProtos.ClusterJobType getType();

    public boolean start(@NotNull final JmxConnect jmxConnect) {
        this.jmxConnect = jmxConnect;

        if (!"NORMAL".equals(jmxConnect.getStorageServiceProxy().getOperationMode())) {
            return false;
        }

        remainingKeyspaces = new CopyOnWriteArrayList<>(jmxConnect.getStorageServiceProxy().getKeyspaces());
        remainingKeyspaces.remove("system");

        return true;
    }

    @Override
    public void close() {
        try {
            checkNotNull(jmxConnect).close();
        } catch (final IOException e) {
            // ignore
        }
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public boolean isFinished() {
        return finishedTimestamp != 0L;
    }

    public long getFinishedTimestamp() {
        return finishedTimestamp;
    }

    @NotNull
    public List<String> getRemainingKeyspaces() {
        if (remainingKeyspaces == null) {
            remainingKeyspaces = newArrayList();
        }
        return remainingKeyspaces;
    }

    @NotNull
    public Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> getKeyspaceStatus() {
        return keyspaceStatus;
    }

    protected void cleanupAfterJobFinished() {
        finishedTimestamp = System.currentTimeMillis();
        final long duration = System.currentTimeMillis() - startTimestamp;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("{} finished in {} seconds : {}",
                getClass().getSimpleName(),
                duration / 1000L,
                keyspaceStatusLogString(keyspaceStatus)
            );
        }
    }

    protected void keyspaceStarted() {
        keyspaceStartedAt = System.currentTimeMillis();
    }

    protected void keyspaceFinished(@NotNull final String status, @NotNull final String keyspace) {
        keyspaceStatus.put(keyspace, CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                .setDuration(System.currentTimeMillis() - keyspaceStartedAt)
                .setStatus(status)
                .setKeyspace(keyspace)
                .build());
    }

    @Nullable
    public String nextKeyspace() {
        if (remainingKeyspaces != null) {
            if (remainingKeyspaces.isEmpty()) {
                cleanupAfterJobFinished();
                return null;
            }

            return remainingKeyspaces.remove(0);
        }
        return null;
    }

    public abstract void startNextKeyspace();

    public void forceAbort() {
        cleanupAfterJobFinished();
    }

    @NotNull
    private static String keyspaceStatusLogString(@NotNull final Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> map) {
        final List<String> strings = newArrayList();
        for (final Map.Entry<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> entry : map.entrySet()) {
            strings.add(String.format("%s -> {%s}", entry.getKey(), protoToString(entry.getValue())));
        }

        if (strings.isEmpty()) {
            return "[]";
        } else {
            return "[{" + JOINER.join(strings) + "}]";
        }
    }
}
