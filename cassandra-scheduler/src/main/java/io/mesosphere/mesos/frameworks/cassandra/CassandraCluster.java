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
package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.FluentIterable.from;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

/**
 * STUB to abstract state management.
 */
public final class CassandraCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCluster.class);

    private static final CassandraCluster INSTANCE = new CassandraCluster();

    private final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> executorMetadataMap = Maps.newConcurrentMap();

    private String name;

    private int jmxPort;
    private boolean jmxSsl;
    private String jmxUsername;
    private String jmxPassword;

    private int storagePort;
    private int sslStoragePort;
    private int nativePort;
    private int rpcPort;

    private final AtomicReference<RepairJob> currentRepair = new AtomicReference<>();
    private volatile RepairJob lastRepair;

    private CassandraCluster() {
    }

    public static CassandraCluster instance() {
        return INSTANCE;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public boolean isJmxSsl() {
        return jmxSsl;
    }

    public void setJmxSsl(boolean jmxSsl) {
        this.jmxSsl = jmxSsl;
    }

    public String getJmxPassword() {
        return jmxPassword;
    }

    public void setJmxPassword(String jmxPassword) {
        this.jmxPassword = jmxPassword;
    }

    public String getJmxUsername() {
        return jmxUsername;
    }

    public void setJmxUsername(String jmxUsername) {
        this.jmxUsername = jmxUsername;
    }

    public int getNativePort() {
        return nativePort;
    }

    public void setNativePort(int nativePort) {
        this.nativePort = nativePort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public void setSslStoragePort(int sslStoragePort) {
        this.sslStoragePort = sslStoragePort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public void setStoragePort(int storagePort) {
        this.storagePort = storagePort;
    }

    public ExecutorMetadata executorMetadata(Protos.ExecutorID executorId) {
        ExecutorMetadata executorMetadata = executorMetadataMap.get(executorId);
        if (executorMetadata == null) {
            ExecutorMetadata existing = executorMetadataMap.putIfAbsent(executorId, executorMetadata = new ExecutorMetadata(executorId));
            if (existing != null)
                executorMetadata = existing;
        }
        return executorMetadata;
    }

    private static final Function<ExecutorMetadata, String> toIp = new Function<ExecutorMetadata, String>() {
        @Override
        public String apply(final ExecutorMetadata input) {
            return input.getSlaveMetadata().getIp();
        }
    };


    public FluentIterable<String> seedsIpList() {
        return from(executorMetadataMap.values()).transform(toIp);
    }

    public List<ExecutorMetadata> allNodes() {
        return new ArrayList<>(executorMetadataMap.values());
    }

    private void repairFinished(RepairJob repairJob) {
        currentRepair.compareAndSet(repairJob, null);
        lastRepair = repairJob;
    }

    public boolean repairStart() {
        return currentRepair.compareAndSet(null, new RepairJob());
    }

    public boolean repairAbort() {
        RepairJob current = currentRepair.get();
        if (current == null)
            return false;
        current.abort();
        return true;
    }

    public RepairJob getLastRepair() {
        return lastRepair;
    }

    public RepairJob getCurrentRepair() {
        return currentRepair.get();
    }

    public boolean shouldStartRepairOnExecutor(Protos.ExecutorID executorID) {
        RepairJob r = currentRepair.get();
        return r != null && r.shouldStartRepairOnExecutor(executorID);
    }

    public boolean shouldGetRepairStatusOnExecutor(Protos.ExecutorID executorID) {
        RepairJob r = currentRepair.get();
        return r != null && r.shouldGetRepairStatusOnExecutor(executorID);
    }

    public void gotRepairStatus(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeRepairStatus repairStatus) {
        RepairJob r = currentRepair.get();
        if (r != null)
            r.gotRepairStatus(executorId, repairStatus);
    }

    public final class RepairJob {
        private static final long REPAIR_STATUS_INVERVAL = 10000L;

        final long startedTimestamp = System.currentTimeMillis();
        Long finishedTimestamp;
        private volatile boolean abort;

        final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> remainingNodes;

        final Map<String, CassandraTaskProtos.CassandraNodeRepairStatus> repairedNodes = Maps.newConcurrentMap();

        volatile ExecutorMetadata current;
        private long nextRepairStatus;

        RepairJob() {
            LOGGER.info("Created repair job");
            remainingNodes = Maps.newConcurrentMap();
            remainingNodes.putAll(executorMetadataMap);
        }

        void gotRepairStatus(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeRepairStatus repairStatus) {
            LOGGER.debug("gotRepairStatus for executor {}: {}", executorId.getValue(), protoToString(repairStatus));
            ExecutorMetadata c = current;
            if (c != null && c.getExecutorId().equals(executorId)) {
                LOGGER.debug("gotRepairStatus for current repair target {}/{} (executor {}) - running:{}",
                        c.getHostname(), c.getIp(), c.getExecutorId().getValue(), repairStatus.getRunning());
                if (!repairStatus.getRunning()) {
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
                if (abort || remainingNodes.isEmpty()) {
                    shutdown();
                    return false;
                }

                ExecutorMetadata candidate = remainingNodes.remove(executorID);

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
            repairFinished(this);
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
            for (ExecutorMetadata executorMetadata : remainingNodes.values()) {
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
}
