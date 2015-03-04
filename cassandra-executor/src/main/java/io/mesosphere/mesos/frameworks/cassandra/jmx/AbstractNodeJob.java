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
package io.mesosphere.mesos.frameworks.cassandra.jmx;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractNodeJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNodeJob.class);

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

    protected void finished() {
        finishedTimestamp = System.currentTimeMillis();
        long duration = System.currentTimeMillis() - startTimestamp;
        LOGGER.info("{} finished in {} seconds : {}", getClass().getSimpleName(), duration / 1000L, keyspaceStatus);
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
            finished();
            return null;
        }

        return remainingKeyspaces.remove(0);
    }

    public abstract void startNextKeyspace();

    public void forceAbort() {
        finished();
    }
}