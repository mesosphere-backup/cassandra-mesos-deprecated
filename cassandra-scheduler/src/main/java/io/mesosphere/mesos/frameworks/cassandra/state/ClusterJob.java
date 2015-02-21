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
package io.mesosphere.mesos.frameworks.cassandra.state;

import com.google.common.collect.Maps;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

/**
 * Base class for all cluster-wide operations.
 */
public abstract class ClusterJob<S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterJob.class);

    protected final CassandraCluster cassandraCluster;

    private final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> remainingNodes;
    private final Map<String, S> processedNodes = Maps.newConcurrentMap();
    private final Set<Protos.ExecutorID> restriction;
    private volatile ExecutorMetadata currentNode;

    private final long startedTimestamp = System.currentTimeMillis();
    private Long finishedTimestamp;
    private long nextStatus;

    private volatile boolean abort;

    public ClusterJob(CassandraCluster cassandraCluster, Set<Protos.ExecutorID> restriction) {
        this.cassandraCluster = cassandraCluster;

        remainingNodes = Maps.newConcurrentMap();
        remainingNodes.putAll(cassandraCluster.executorMetadataMap);

        this.restriction = restriction;
    }

    public void started() {
        LOGGER.info("Created {}", this.getClass().getSimpleName());
    }

    public boolean hasRemainingNodes() {
        return !remainingNodes.isEmpty();
    }

    public Collection<ExecutorMetadata> allRemainingNodes() {
        return remainingNodes.values();
    }

    protected ExecutorMetadata remainingNodeForExecutor(Protos.ExecutorID executorID) {
        return remainingNodes.remove(executorID);
    }

    protected void shutdown() {
        LOGGER.info("Shutting down repair job");
        finishedTimestamp = System.currentTimeMillis();
        cassandraCluster.clusterJobFinished(this);
    }

    public void abort() {
        abort = true;
    }

    public boolean isAborted() {
        return abort;
    }

    public long getStartedTimestamp() {
        return startedTimestamp;
    }

    public Long getFinishedTimestamp() {
        return finishedTimestamp;
    }

    void gotStatusFromExecutor(Protos.ExecutorID executorId, S status) {
        LOGGER.debug("gotRepairStatus for executor {}: {}", executorId.getValue(), protoToString(status));
        ExecutorMetadata c = currentNode;
        if (c != null && c.getExecutorId().equals(executorId)) {
            boolean finished = statusIsFinished(status);
            LOGGER.debug("gotRepairStatus for current repair target {}/{} (executor {}) - running:{}",
                    c.getHostname(), c.getIp(), c.getExecutorId().getValue(), !finished);
            if (finished) {
                c.repairDone(cassandraCluster.clock.now().getMillis());
                processedNodes.put(c.getIp(), status);
                currentNode = null;
            }
        }
    }

    boolean shouldStartOnExecutor(Protos.ExecutorID executorID) {
        if (currentNode == null) {
            if (abort || !hasRemainingNodes()) {
                shutdown();
                return false;
            }

            if (restriction != null && !restriction.contains(executorID))
                return false;

            ExecutorMetadata candidate = remainingNodeForExecutor(executorID);

            if (candidate != null) {
                CassandraTaskProtos.CassandraNodeHealthCheckDetails hc = candidate.getLastHealthCheckDetails();
                if (checkNodeStatus(hc)) {
                    LOGGER.info("moving current {} target to {}/{} (executor {})", getClass().getSimpleName(),
                            candidate.getHostname(), candidate.getIp(), candidate.getExecutorId().getValue());
                    currentNode = candidate;
                    scheduleNextStatus();

                    return true;
                } else
                    LOGGER.info("skipping {}/{} for {} (executor {})",
                            candidate.getHostname(), candidate.getIp(), getClass().getSimpleName(),
                            candidate.getExecutorId().getValue());
            }
        }
        return false;
    }

    protected boolean checkNodeStatus(CassandraTaskProtos.CassandraNodeHealthCheckDetails hc) {
        return hc.getHealthy();
    }

    public boolean shouldGetStatusFromExecutor(Protos.ExecutorID executorID) {
        ExecutorMetadata c = currentNode;
        if (c != null && c.getExecutorId().equals(executorID)) {
            if (nextStatus <= System.currentTimeMillis()) {
                scheduleNextStatus();
                return true;
            }
        }
        return false;
    }

    protected void scheduleNextStatus() {
        nextStatus = System.currentTimeMillis() + statusInterval();
    }

    protected abstract long statusInterval();

    protected abstract boolean statusIsFinished(S status);

    public Map<String, S> getProcessedNodes() {
        return processedNodes;
    }

    public String getCurrentNodeIp() {
        return currentNode != null ? currentNode.getIp() : null;
    }

    public List<String> getRemainingNodeIps() {
        List<String> ips = new ArrayList<>();
        for (ExecutorMetadata executorMetadata : allRemainingNodes()) {
            String ip = executorMetadata.getIp();
            if (ip != null)
                ips.add(ip);
        }
        return ips;
    }
}
