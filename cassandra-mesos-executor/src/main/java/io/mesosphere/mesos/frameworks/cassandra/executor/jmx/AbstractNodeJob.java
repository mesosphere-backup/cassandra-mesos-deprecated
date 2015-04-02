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
import com.google.common.collect.Lists;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public abstract class AbstractNodeJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeJob.class);

    private static final Joiner JOINER = Joiner.on("},{");

    protected final long startTimestamp = System.currentTimeMillis();

    private final Protos.TaskID taskId;

    protected JmxConnect jmxConnect;
    private List<String> remainingKeyspaces;
    private final Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> keyspaceStatus = new HashMap<>();

    private volatile long keyspaceStartedAt;

    private long finishedTimestamp;

    protected AbstractNodeJob(Protos.TaskID taskId) {
        this.taskId = taskId;
    }

    public Protos.TaskID getTaskId() {
        return taskId;
    }

    public abstract CassandraFrameworkProtos.ClusterJobType getType();

    public boolean start(JmxConnect jmxConnect) {
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
            jmxConnect.close();
        } catch (IOException e) {
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

    public List<String> getRemainingKeyspaces() {
        return remainingKeyspaces;
    }

    public Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> getKeyspaceStatus() {
        return keyspaceStatus;
    }

    protected void cleanupAfterJobFinished() {
        finishedTimestamp = System.currentTimeMillis();
        long duration = System.currentTimeMillis() - startTimestamp;
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

    protected void keyspaceFinished(String status, String keyspace) {
        keyspaceStatus.put(keyspace, CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                .setDuration(System.currentTimeMillis() - keyspaceStartedAt)
                .setStatus(status)
                .setKeyspace(keyspace)
                .build());
    }

    public String nextKeyspace() {
        if (remainingKeyspaces.isEmpty()) {
            cleanupAfterJobFinished();
            return null;
        }

        return remainingKeyspaces.remove(0);
    }

    public abstract void startNextKeyspace();

    public void forceAbort() {
        cleanupAfterJobFinished();
    }

    private static String keyspaceStatusLogString(@NotNull final Map<String, CassandraFrameworkProtos.ClusterJobKeyspaceStatus> map) {
        final List<String> strings = Lists.newArrayList();
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
