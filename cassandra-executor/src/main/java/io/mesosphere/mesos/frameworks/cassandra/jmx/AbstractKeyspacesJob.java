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

import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractKeyspacesJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKeyspacesJob.class);

    protected final long startTimestamp = System.currentTimeMillis();

    protected JmxConnect jmxConnect;
    private List<String> keyspaces;
    private final Map<String, CassandraTaskProtos.JobKeyspaceStatus> keyspaceStatus = new HashMap<>();

    private volatile long keyspaceStartedAt;

    private long finishedTimestamp;

    protected AbstractKeyspacesJob() {

    }

    public abstract CassandraTaskProtos.KeyspaceJobType getType();

    public boolean start(JmxConnect jmxConnect) {
        this.jmxConnect = jmxConnect;

        if (!"NORMAL".equals(jmxConnect.getStorageServiceProxy().getOperationMode()))
            return false;

        keyspaces = new CopyOnWriteArrayList<>(jmxConnect.getStorageServiceProxy().getKeyspaces());
        keyspaces.remove("system");
//        keyspaces.remove("system_auth");
//        keyspaces.remove("system_traces");

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
        return keyspaces;
    }

    public Map<String, CassandraTaskProtos.JobKeyspaceStatus> getKeyspaceStatus() {
        return keyspaceStatus;
    }

    protected void finished() {
        finishedTimestamp = System.currentTimeMillis();
        long duration = System.currentTimeMillis() - startTimestamp;
        LOGGER.info("{} finished in {} seconds : {}", getClass().getSimpleName(), duration / 1000L, keyspaceStatus);
    }

    protected List<String> getKeyspaces() {
        return keyspaces;
    }

    protected void keyspaceStarted() {
        keyspaceStartedAt = System.currentTimeMillis();
    }

    protected void keyspaceFinished(String status, String keyspace) {
        keyspaceStatus.put(keyspace, CassandraTaskProtos.JobKeyspaceStatus.newBuilder()
                .setDuration(System.currentTimeMillis() - keyspaceStartedAt)
                .setStatus(status)
                .setKeyspace(keyspace)
                .build());
    }

    public String nextKeyspace() {
        if (keyspaces.isEmpty()) {
            finished();
            return null;
        }

        return keyspaces.remove(0);
    }

    public abstract void startNextKeyspace();
}