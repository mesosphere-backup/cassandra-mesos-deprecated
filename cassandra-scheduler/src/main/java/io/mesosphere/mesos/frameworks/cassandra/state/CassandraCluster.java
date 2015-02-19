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

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import io.mesosphere.mesos.frameworks.cassandra.ExecutorMetadata;
import io.mesosphere.mesos.util.Clock;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.Protos;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.Functions.headOption;
import static io.mesosphere.mesos.util.Functions.unmodifiableHashMap;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

/**
 * STUB to abstract state management.
 */
public final class CassandraCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCluster.class);

    // see: http://www.datastax.com/documentation/cassandra/2.1/cassandra/security/secureFireWall_r.html
    private static final Map<String, Long> defaultCassandraPortMappings = unmodifiableHashMap(
            tuple2("storage_port", 7000L),
            tuple2("ssl_storage_port", 7001L),
            tuple2("jmx", 7199L),
            tuple2("native_transport_port", 9042L),
            tuple2("rpc_port", 9160L)
    );

    public static final int DEFAULT_SEED_NODE_COUNT = 3;

    final Clock clock = new SystemClock();

    final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> executorMetadataMap = Maps.newConcurrentMap();
    private final Set<Protos.ExecutorID> seedNodeExecutors = Sets.newConcurrentHashSet();
    private final Map<Protos.TaskID, Protos.ExecutorID> taskToExecutor = Maps.newConcurrentMap();

    private String name;
    private double cpuCores;
    private long memMb;
    private long diskMb;
    private int nodeCount;
    private int seedNodeCount;

    private long nextNodeAddTime;

    private boolean jmxSsl;
    private String jmxUsername;
    private String jmxPassword;

    private int jmxPort;
    private int storagePort;
    private int sslStoragePort;
    private int nativePort;
    private int rpcPort;

    private long healthCheckIntervalMillis;

    private long bootstrapGraceTimeMillis = TimeUnit.MINUTES.toMillis(2);

    private final AtomicReference<ClusterJob> currentClusterJob = new AtomicReference<>();

    private volatile RepairJob lastRepair;

    private String cassandraVersion;

    private static final AtomicReference<CassandraCluster> singleton = new AtomicReference<>();

    public CassandraCluster() {
        if (!singleton.compareAndSet(null, this))
            throw new RuntimeException("oops");
    }

    public static CassandraCluster singleton() {
        return singleton.get();
    }

    public void configure(String name, int nodeCount,
                          Duration healthCheckInterval,
                          Duration bootstrapGraceTime,
                          double cpuCores, long memMb, long diskMb, String cassandraVersion) {
        this.name = name;
        this.nodeCount = nodeCount;

        this.cpuCores = cpuCores;
        this.memMb = memMb;
        this.diskMb = diskMb;

        this.cassandraVersion = cassandraVersion;

        this.jmxPort = defaultCassandraPortMappings.get("jmx").intValue();
        this.nativePort = defaultCassandraPortMappings.get("native_transport_port").intValue();
        this.rpcPort = defaultCassandraPortMappings.get("rpc_port").intValue();
        this.storagePort = defaultCassandraPortMappings.get("storage_port").intValue();
        this.sslStoragePort = defaultCassandraPortMappings.get("ssl_storage_port").intValue();

        this.seedNodeCount = Math.min(DEFAULT_SEED_NODE_COUNT, nodeCount);

        this.healthCheckIntervalMillis = healthCheckInterval.getMillis();
        this.bootstrapGraceTimeMillis = bootstrapGraceTime.getMillis();
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

    public String getCassandraVersion() {
        return cassandraVersion;
    }

    public String getName() {
        return name;
    }

    public int getNativePort() {
        return nativePort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public int getStoragePort() {
        return storagePort;
    }

    //

    public ExecutorMetadata allocateNewExecutor(String hostname) {
        for (ExecutorMetadata executorMetadata : executorMetadataMap.values())
            if (hostname.equals(executorMetadata.getHostname()))
                return null;

        Protos.ExecutorID executorId = executorId(name + ".node." + hostname + ".executor");

        ExecutorMetadata executorMetadata = new ExecutorMetadata(executorId);
        ExecutorMetadata existing = executorMetadataMap.putIfAbsent(executorId, executorMetadata);
        if (existing != null)
            assert false;
        else
            executorMetadata.updateHostname(hostname, jmxPort);

        LOGGER.debug("Allocated new executor {} on host {}/{}", executorId.getValue(), hostname, executorMetadata.getIp());

        return executorMetadata;
    }

    public ExecutorMetadata metadataForExecutor(Protos.ExecutorID executorId) {
        return executorId != null ? executorMetadataMap.get(executorId) : null;
    }

    public ExecutorMetadata metadataForTask(Protos.TaskID taskId) {
        return metadataForExecutor(taskToExecutor.get(taskId));
    }

    public ExecutorMetadata unassociateTaskId(Protos.TaskID taskId) {
        Protos.ExecutorID executorId = taskToExecutor.remove(taskId);
        LOGGER.debug("removing taskId {} from executor {}", taskId.getValue(), executorId != null ? executorId.getValue() : null);
        return metadataForExecutor(executorId);
    }

    public Protos.TaskID createTaskId(ExecutorMetadata executorMetadata, String suffix) {
        return associateTaskId(executorMetadata, executorMetadata.createTaskId(suffix));
    }

    public Protos.TaskID associateTaskId(ExecutorMetadata executorMetadata, Protos.TaskID taskId) {
        LOGGER.debug("associated taskId {} to executor {}", taskId.getValue(), executorMetadata.getExecutorId().getValue());
        taskToExecutor.put(taskId, executorMetadata.getExecutorId());
        return taskId;
    }

    public void executorLost(Protos.ExecutorID executorId) {
        LOGGER.debug("executor {} lost", executorId.getValue());

        for (Iterator<Map.Entry<Protos.TaskID, Protos.ExecutorID>> iter = taskToExecutor.entrySet().iterator();
             iter.hasNext(); ) {
            Map.Entry<Protos.TaskID, Protos.ExecutorID> entry = iter.next();
            if (entry.getValue().equals(executorId)) {
                LOGGER.debug("executor {} lost with task {}", executorId.getValue(), entry.getKey().getValue());
                iter.remove();
            }
        }

        ExecutorMetadata executorMetadata = executorMetadataMap.remove(executorId);
        if (executorMetadata != null)
            executorMetadata.executorLost();

        // TODO check repair + cleanup jobs
    }

    //

    public void makeSeed(ExecutorMetadata executorMetadata) {
        seedNodeExecutors.add(executorMetadata.getExecutorId());
    }

    public boolean hasRequiredSeedNodes() {
        return seedNodeExecutors.size() >= seedNodeCount;
    }

    public boolean hasRequiredNodes() {
        return executorMetadataMap.size() >= nodeCount;
    }

    public boolean canAddNode() {
        return nextNodeAddTime < clock.now().getMillis();
    }

    public void nodeInitializing() {
        nextNodeAddTime = Long.MAX_VALUE;
    }

    public void nodeRunning() {
        nextNodeAddTime = clock.now().getMillis() + bootstrapGraceTimeMillis;
    }

    public long getNextNodeLaunchTime() {
        return nextNodeAddTime;
    }

    public long getBootstrapGraceTimeMillis() {
        return bootstrapGraceTimeMillis;
    }

    public boolean shouldRunHealthCheck(Protos.ExecutorID executorID) {
        ExecutorMetadata executorMetadata = metadataForExecutor(executorID);

        return executorMetadata != null
                && clock.now().getMillis() > executorMetadata.getLastHealthCheck() + healthCheckIntervalMillis;

    }

    public long getHealthCheckIntervalMillis() {
        return healthCheckIntervalMillis;
    }

    public int getSeedNodeCount() {
        return seedNodeCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public Iterable<String> seedsIpList() {
        List<String> ips = new ArrayList<>();
        for (Protos.ExecutorID seedNodeExecutor : seedNodeExecutors) {
            ExecutorMetadata executorMetadata = executorMetadataMap.get(seedNodeExecutor);
            if (executorMetadata.getIp() != null)
                ips.add(executorMetadata.getIp());
        }
        return ips;
    }

    public List<ExecutorMetadata> allNodes() {
        return new ArrayList<>(executorMetadataMap.values());
    }

    public int updateNodeCount(int nodeCount) {
        if (nodeCount < seedNodeCount)
            return this.nodeCount;
        this.nodeCount = nodeCount;
        return nodeCount;
    }

    public <J extends ClusterJob> J currentClusterJob() {
        return (J) currentClusterJob.get();
    }

    void repairFinished(RepairJob repairJob) {
        currentClusterJob.compareAndSet(repairJob, null);
        lastRepair = repairJob;
    }

    public boolean repairStart() {
        return currentClusterJob.compareAndSet(null, new RepairJob(this));
    }

    public boolean repairAbort() {
        RepairJob current = getCurrentRepair();
        if (current == null)
            return false;
        current.abort();
        return true;
    }

    public RepairJob getLastRepair() {
        return lastRepair;
    }

    public RepairJob getCurrentRepair() {
        ClusterJob current = currentClusterJob.get();
        return (current instanceof RepairJob) ? (RepairJob)current : null;
    }

    public boolean shouldStartRepairOnExecutor(Protos.ExecutorID executorID) {
        RepairJob r = getCurrentRepair();
        return r != null && r.shouldStartRepairOnExecutor(executorID);
    }

    public boolean shouldGetRepairStatusOnExecutor(Protos.ExecutorID executorID) {
        RepairJob r = getCurrentRepair();
        return r != null && r.shouldGetRepairStatusOnExecutor(executorID);
    }

    public void gotRepairStatus(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeRepairStatus repairStatus) {
        RepairJob r = getCurrentRepair();
        if (r != null)
            r.gotRepairStatus(executorId, repairStatus);
    }

    public void updateHealthCheck(Protos.ExecutorID executorId, CassandraTaskProtos.CassandraNodeHealthCheckDetails healthCheckDetails) {
        ExecutorMetadata executorMetadata = metadataForExecutor(executorId);
        if (executorMetadata != null)
            executorMetadata.updateHealthCheck(clock.now().getMillis(), healthCheckDetails);
    }

    public double getCpuCores() {
        return cpuCores;
    }

    public long getDiskMb() {
        return diskMb;
    }

    public long getMemMb() {
        return memMb;
    }

    public Iterable<? extends Protos.Resource> resourcesForExecutor(ExecutorMetadata executorMetadata) {
        return newArrayList(
                cpu(cpuCores),
                mem(memMb),
                disk(diskMb),
                ports(portMapping(executorMetadata).values())
        );
    }

    public Map<String, Long> portMapping(ExecutorMetadata executorMetadata) {
        Map<String, Long> result = new HashMap<>(defaultCassandraPortMappings);
//        result.put("jmx", (long) executorMetadata.getJmxPort());
        return result;
    }

    public List<String> checkResources(Protos.Offer offer, ExecutorMetadata executorMetadata) {
        List<String> errors = newArrayList();

        ListMultimap<String, Protos.Resource> index = from(offer.getResourcesList()).index(resourceToName());

        Double availableCpus = resourceValueDouble(headOption(index.get("cpus"))).or(0.0);
        Long availableMem = resourceValueLong(headOption(index.get("mem"))).or(0L);
        Long availableDisk = resourceValueLong(headOption(index.get("disk"))).or(0L);
        if (availableCpus <= cpuCores) {
            errors.add(String.format("Not enough cpu resources. Required %f only %f available.", cpuCores, availableCpus));
        }
        if (availableMem <= memMb) {
            errors.add(String.format("Not enough mem resources. Required %d only %d available", memMb, availableMem));
        }
        if (availableDisk <= diskMb) {
            errors.add(String.format("Not enough disk resources. Required %d only %d available", diskMb, availableDisk));
        }

        TreeSet<Long> ports = resourceValueRange(headOption(index.get("ports")));
        for (Map.Entry<String, Long> entry : portMapping(executorMetadata).entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            if (!ports.contains(value))
                errors.add(String.format("Unavailable port %d(%s). %d other ports available.", value, key, ports.size()));
        }
        return errors;
    }
}
