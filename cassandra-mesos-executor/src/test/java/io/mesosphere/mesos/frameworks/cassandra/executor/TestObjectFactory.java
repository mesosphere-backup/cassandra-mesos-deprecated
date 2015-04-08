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
package io.mesosphere.mesos.frameworks.cassandra.executor;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.jetbrains.annotations.NotNull;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Marker;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.TabularData;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This implementation allows to mock Cassandra for executor's use case.
 */
class TestObjectFactory implements ObjectFactory {
    final String hostId = UUID.randomUUID().toString();
    final MockStorageService storageServiceProxy = new MockStorageService();
    final MockEndpointSnitchInfo endpointSnitchInfo = new MockEndpointSnitchInfo();
    TestWrappedProcess process;

    @NotNull
    @Override
    public JmxConnect newJmxConnect(@NotNull final CassandraFrameworkProtos.JmxConnect jmx) {
        return new TestJmxConnect();
    }

    @NotNull
    @Override
    public WrappedProcess launchCassandraNodeTask(@NotNull final Marker taskIdMarker, @NotNull final CassandraFrameworkProtos.CassandraServerRunTask cassandraServerRunTask) throws LaunchNodeException {
        return process = new TestWrappedProcess();
    }

    @Override
    public void updateCassandraServerConfig(@NotNull final Marker taskIdMarker, @NotNull final CassandraFrameworkProtos.CassandraServerRunTask cassandraServerRunTask, @NotNull final CassandraFrameworkProtos.UpdateConfigTask updateConfigTask) {
        // nop
    }

    private static class TestWrappedProcess implements WrappedProcess {

        int exitCode = -1;

        @Override
        public int getPid() {
            return 42;
        }

        @Override
        public void destroy() {
            daemonStopped();
        }

        @Override
        public void destroyForcibly() {
            // nop
        }

        @Override
        public int exitValue() {
            if (exitCode == -1)
                throw new IllegalThreadStateException();
            return exitCode;
        }

        public void daemonStopped() {
            exitCode = 0;
        }
    }

    final class TestJmxConnect implements JmxConnect {

        @NotNull
        @Override
        public StorageServiceMBean getStorageServiceProxy() {
            return storageServiceProxy;
        }

        @NotNull
        @Override
        public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy() {
            return endpointSnitchInfo;
        }

        @NotNull
        @Override
        public RuntimeMXBean getRuntimeProxy() {
            return ManagementFactory.getRuntimeMXBean();
        }

        @Override
        @NotNull
        public List<String> getColumnFamilyNames(@NotNull final String keyspace) {
            return Arrays.asList(keyspace + "_a", keyspace + "_b", keyspace + "_c");
        }

        @Override
        public void close() {
            //
        }

    }

    final class MockStorageService implements StorageServiceMBean {

        @Override
        public String getOperationMode() {
            return "NORMAL";
        }

        @Override
        public boolean isGossipRunning() {
            return true;
        }

        @Override
        public boolean isJoined() {
            return true;
        }

        @Override
        public boolean isNativeTransportRunning() {
            return true;
        }

        @Override
        public boolean isRPCServerRunning() {
            return true;
        }

        @Override
        public boolean isInitialized() {
            return true;
        }

        @Override
        public boolean isStarting() {
            return false;
        }

        @Override
        public String getReleaseVersion() {
            return "5.6.7";
        }

        @Override
        public String getClusterName() {
            return "mocked-unit-test";
        }

        @Override
        public List<String> getTokens() {
            return Arrays.asList("1", "2");
        }

        @Override
        public List<String> getTokens(final String endpoint) {
            return Arrays.asList("1", "2");
        }

        @Override
        public Map<String, String> getTokenToEndpointMap() {
            return Collections.singletonMap("1", "1.2.3.4");
        }

        @Override
        public String getLocalHostId() {
            return hostId;
        }

        @Override
        public List<String> getKeyspaces() {
            return Arrays.asList("system", "foo", "bar", "baz");
        }

        //

        final List<NotificationListener> listeners = new ArrayList<>();

        @Override
        public void removeNotificationListener(final NotificationListener listener, final NotificationFilter filter, final Object handback) {
            listeners.remove(listener);
        }

        @Override
        public void addNotificationListener(final NotificationListener listener, final NotificationFilter filter, final Object handback) throws IllegalArgumentException {
            listeners.add(listener);
        }

        @Override
        public void removeNotificationListener(final NotificationListener listener) {
            listeners.remove(listener);
        }

        //

        int commandSeq;
        long sequence;

        @Override
        public int forceRepairAsync(final String keyspace, final boolean isSequential, final boolean isLocal, final boolean primaryRange, final boolean fullRepair, final String... columnFamilies) {
            return ++commandSeq;
        }


        public void emitRepairNotification(final ActiveRepairService.Status status) {
            final Notification notification = new Notification("repair", this, ++sequence, System.currentTimeMillis(), "hello world");
            notification.setUserData(new int[]{commandSeq, status.ordinal()});
            emitNotification(notification);
        }

        public void emitNotification(final Notification notification) {
            for (final NotificationListener listener : new ArrayList<>(listeners)) {
                listener.handleNotification(notification, null);
            }
        }

        @Override
        public void stopDaemon() {
            process.daemonStopped();
        }

        //

        @Override
        public int forceKeyspaceCleanup(final String keyspaceName, final String... columnFamilies) {
            return CompactionManager.AllSSTableOpStatus.SUCCESSFUL.statusCode;
        }

        //

        @Override
        public List<String> getLiveNodes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getUnreachableNodes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getJoiningNodes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getLeavingNodes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getMovingNodes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSchemaVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getAllDataFileLocations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getCommitLogLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSavedCachesLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<List<String>, List<String>> getRangeToEndpointMap(final String keyspace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<List<String>, List<String>> getRangeToRpcaddressMap(final String keyspace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> describeRingJMX(final String keyspace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<List<String>, List<String>> getPendingRangeToEndpointMap(final String keyspace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getHostIdMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getLoad() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getLoadString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getLoadMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCurrentGenerationNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InetAddress> getNaturalEndpoints(final String keyspaceName, final String cf, final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InetAddress> getNaturalEndpoints(final String keyspaceName, final ByteBuffer key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void takeSnapshot(final String tag, final String... keyspaceNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void takeColumnFamilySnapshot(final String keyspaceName, final String columnFamilyName, final String tag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearSnapshot(final String tag, final String... keyspaceNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, TabularData> getSnapshotDetails() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long trueSnapshotsSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceKeyspaceCompaction(final String keyspaceName, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int scrub(final boolean disableSnapshot, final boolean skipCorrupted, final String keyspaceName, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int upgradeSSTables(final String keyspaceName, final boolean excludeCurrentVersion, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceKeyspaceFlush(final String keyspaceName, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int forceRepairAsync(final String keyspace, final boolean isSequential, final Collection<String> dataCenters, final Collection<String> hosts, final boolean primaryRange, final boolean repairedAt, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int forceRepairAsync(final String keyspace, final int parallelismDegree, final Collection<String> dataCenters, final Collection<String> hosts, final boolean primaryRange, final boolean fullRepair, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int forceRepairRangeAsync(final String beginToken, final String endToken, final String keyspaceName, final boolean isSequential, final boolean isLocal, final boolean repairedAt, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int forceRepairRangeAsync(final String beginToken, final String endToken, final String keyspaceName, final boolean isSequential, final Collection<String> dataCenters, final Collection<String> hosts, final boolean repairedAt, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int forceRepairRangeAsync(final String beginToken, final String endToken, final String keyspaceName, final int parallelismDegree, final Collection<String> dataCenters, final Collection<String> hosts, final boolean fullRepair, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceTerminateAllRepairSessions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void decommission() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void move(final String newToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeNode(final String token) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getRemovalStatus() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceRemoveCompletion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLoggingLevel(final String classQualifier, final String level) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getLoggingLevels() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDrainProgress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drain() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(final String keyspace, final String columnFamily) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<InetAddress, Float> getOwnership() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<InetAddress, Float> effectiveOwnership(final String keyspace) throws IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateSnitch(final String epSnitchClassName, final Boolean dynamic, final Integer dynamicUpdateInterval, final Integer dynamicResetInterval, final Double dynamicBadnessThreshold) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopGossiping() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startGossiping() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopRPCServer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startRPCServer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopNativeTransport() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startNativeTransport() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void joinRing() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getExceptionCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setStreamThroughputMbPerSec(final int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStreamThroughputMbPerSec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCompactionThroughputMbPerSec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCompactionThroughputMbPerSec(final int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isIncrementalBackupsEnabled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setIncrementalBackupsEnabled(final boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rebuild(final String sourceDc) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulkLoad(final String directory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String bulkLoadAsync(final String directory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rescheduleFailedDeletions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void loadNewSSTables(final String ksName, final String cfName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> sampleKeyRange() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rebuildSecondaryIndex(final String ksName, final String cfName, final String... idxNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetLocalSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTraceProbability(final double probability) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getTracingProbability() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disableAutoCompaction(final String ks, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableAutoCompaction(final String ks, final String... columnFamilies) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deliverHints(final String host) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPartitionerName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTombstoneWarnThreshold() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTombstoneWarnThreshold(final int tombstoneDebugThreshold) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTombstoneFailureThreshold() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTombstoneFailureThreshold(final int tombstoneDebugThreshold) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setHintedHandoffThrottleInKB(final int throttleInKB) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MBeanNotificationInfo[] getNotificationInfo() {
            throw new UnsupportedOperationException();
        }
    }

    static final class MockEndpointSnitchInfo implements EndpointSnitchInfoMBean {
        @Override
        public String getRack(final String host) {
            return "rack";
        }

        @Override
        public String getDatacenter(final String host) {
            return "datacenter";
        }

        @Override
        public String getSnitchName() {
            return "mock-snitch";
        }
    }
}
