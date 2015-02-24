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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class RepairJob extends ClusterJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepairJob.class);

    private static final long REPAIR_STATUS_INVERVAL = 10000L;

    private final long startedTimestamp = System.currentTimeMillis();
    private Long finishedTimestamp;
    private volatile boolean abort;

    private final Map<String, CassandraTaskProtos.CassandraNodeRepairStatus> repairedNodes = Maps.newConcurrentMap();

    private volatile ExecutorMetadata current;
    private long nextRepairStatus;

    RepairJob(CassandraCluster cassandraCluster) {
        super(cassandraCluster);

        LOGGER.info("Created repair job");
    }

    void gotRepairStatus(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeRepairStatus repairStatus) {
        LOGGER.debug("gotRepairStatus for executor {}: {}", executorId.getValue(), protoToString(repairStatus));
        ExecutorMetadata c = current;
        if (c != null && c.getExecutorId().equals(executorId)) {
            LOGGER.debug("gotRepairStatus for current repair target {}/{} (executor {}) - running:{}",
                    c.getHostname(), c.getIp(), c.getExecutorId().getValue(), repairStatus.getRunning());
            if (!repairStatus.getRunning()) {
                c.repairDone(cassandraCluster.clock.now().getMillis());
                repairedNodes.put(c.getIp(), repairStatus);
                current = null;
            }
        }
    }

    public boolean shouldGetRepairStatusOnExecutor(Protos.ExecutorID executorID) {
        ExecutorMetadata c = current;
        if (c != null && c.getExecutorId().equals(executorID)) {
            if (nextRepairStatus <= System.currentTimeMillis()) {
                scheduleNextRepairStatus();
                return true;
            }
        }
        return false;
    }

    private void scheduleNextRepairStatus() {
        nextRepairStatus = System.currentTimeMillis() + REPAIR_STATUS_INVERVAL;
    }

    boolean shouldStartRepairOnExecutor(Protos.ExecutorID executorID) {
        if (current == null) {
            if (abort || !hasRemainingNodes()) {
                shutdown();
                return false;
            }

            ExecutorMetadata candidate = remainingNodeForExecutor(executorID);

            if (candidate != null) {
                CassandraTaskProtos.CassandraNodeHealthCheckDetails hc = candidate.getLastHealthCheckDetails();
                if (hc.getHealthy()) {
                    LOGGER.debug("moving current repair target to {}/{} (executor {})",
                            candidate.getHostname(), candidate.getIp(), candidate.getExecutorId().getValue());
                    current = candidate;
                    scheduleNextRepairStatus();

                    return true;
                }
            }
        }
        return false;
    }

    private void shutdown() {
        LOGGER.info("Shutting down repair job");
        finishedTimestamp = System.currentTimeMillis();
        cassandraCluster.repairFinished(this);
    }

    public void abort() {
        abort = true;
    }

    public boolean isAborted() {
        return abort;
    }

    public Map<String, CassandraTaskProtos.CassandraNodeRepairStatus> getRepairedNodes() {
        return repairedNodes;
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

    public String getCurrentNodeIp() {
        return current != null ? current.getIp() : null;
    }

    public long getStartedTimestamp() {
        return startedTimestamp;
    }

    public Long getFinishedTimestamp() {
        return finishedTimestamp;
    }
}
