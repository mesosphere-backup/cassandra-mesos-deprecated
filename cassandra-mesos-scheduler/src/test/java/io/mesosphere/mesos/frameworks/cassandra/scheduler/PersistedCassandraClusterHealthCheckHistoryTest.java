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

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraClusterHealthCheckHistory;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

public class PersistedCassandraClusterHealthCheckHistoryTest {

    @Test
    public void testRecord() throws Exception {
        State state = new InMemoryState();
        PersistedCassandraClusterHealthCheckHistory hcHistory = new PersistedCassandraClusterHealthCheckHistory(state);

        String exec1 = "exec1";

        CassandraFrameworkProtos.NodeInfo.Builder ni = CassandraFrameworkProtos.NodeInfo.newBuilder()
            .setClusterName("cluster")
            .setDataCenter("dc1")
            .setUptimeMillis(1);
        CassandraFrameworkProtos.HealthCheckDetails.Builder hc = CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
            .setHealthy(true)
            .setInfo(ni.build());
        hcHistory.record(exec1, 1L, hc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry hce1 = hcHistory.last(exec1);
        assertNotNull(hce1);
        assertThat(hcHistory.entries())
            .hasSize(1)
            .contains(hce1);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(1)
            .contains(hce1);
        assertEquals(1L, hce1.getTimestampStart());
        assertEquals(1L, hce1.getTimestampEnd());

        // just increase uptime
        hc.setInfo(ni
            .setUptimeMillis(2)
            .build());
        hcHistory.record(exec1, 2L, hc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry hce2 = hcHistory.last(exec1);
        assertNotNull(hce2);
        assertThat(hcHistory.entries())
            .hasSize(1)
            .contains(hce2);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(1)
            .contains(hce2);
        assertEquals(1L, hce2.getTimestampStart());
        assertEquals(2L, hce2.getTimestampEnd());

        // toggle a field
        hc.setInfo(ni
            .setNativeTransportRunning(true)
            .build());
        hcHistory.record(exec1, 3L, hc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry hce3 = hcHistory.last(exec1);
        assertNotNull(hce3);
        assertThat(hcHistory.entries())
            .hasSize(2)
            .contains(hce2)
            .contains(hce3);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(2)
            .contains(hce2)
            .contains(hce3);
        assertEquals(3L, hce3.getTimestampStart());
        assertEquals(3L, hce3.getTimestampEnd());

        // toggle more fields and record
        hc.setInfo(ni
            .setNativeTransportRunning(false)
            .setRpcServerRunning(true)
            .build());
        hcHistory.record(exec1, 4L, hc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry hce4 = hcHistory.last(exec1);
        assertThat(hcHistory.entries()).hasSize(3);

        hc.setInfo(ni
            .setNativeTransportRunning(true)
            .setRpcServerRunning(true)
            .build());
        hcHistory.record(exec1, 5L, hc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry hce5 = hcHistory.last(exec1);
        assertThat(hcHistory.entries()).hasSize(4);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(4)
            .contains(hce2)
            .contains(hce3)
            .contains(hce4)
            .contains(hce5);

        hc.setHealthy(true)
            .setMsg("msg");
        hcHistory.record(exec1, 6L, hc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry hce6 = hcHistory.last(exec1);
        assertThat(hcHistory.entries()).hasSize(5);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(5)
            .contains(hce2)
            .contains(hce3)
            .contains(hce4)
            .contains(hce5)
            .contains(hce6);

        hc.setHealthy(false)
            .setMsg("foo");
        hcHistory.record(exec1, 7L, hc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry hce7 = hcHistory.last(exec1);
        assertThat(hcHistory.entries()).hasSize(5);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(5)
            .doesNotContain(hce2)
            .contains(hce3)
            .contains(hce4)
            .contains(hce5)
            .contains(hce6)
            .contains(hce7);

        hc.setHealthy(true)
            .clearMsg();
        hcHistory.record(exec1, 8L, hc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry hce8 = hcHistory.last(exec1);
        assertThat(hcHistory.entries()).hasSize(5);
        assertThat(hcHistory.entriesForExecutor(exec1))
            .hasSize(5)
            .doesNotContain(hce2)
            .doesNotContain(hce3)
            .contains(hce4)
            .contains(hce5)
            .contains(hce6)
            .contains(hce7)
            .contains(hce8);

        //
        // similar for another executor
        //

        String exec2 = "exec2";

        CassandraFrameworkProtos.NodeInfo.Builder otherNi = CassandraFrameworkProtos.NodeInfo.newBuilder()
            .setClusterName("cluster")
            .setDataCenter("dc1")
            .setUptimeMillis(1);
        CassandraFrameworkProtos.HealthCheckDetails.Builder otherHc = CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
            .setHealthy(true)
            .setInfo(otherNi.build());
        hcHistory.record(exec2, 1L, otherHc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce1 = hcHistory.last(exec2);
        assertNotNull(otherHce1);
        assertThat(hcHistory.entries())
            .hasSize(6)
            .contains(otherHce1)
            //
            .doesNotContain(hce2)
            .doesNotContain(hce3)
            .contains(hce4)
            .contains(hce5)
            .contains(hce6)
            .contains(hce7)
            .contains(hce8);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(1)
            .contains(otherHce1);
        assertEquals(1L, otherHce1.getTimestampStart());
        assertEquals(1L, otherHce1.getTimestampEnd());

        // just increase uptime
        otherHc.setInfo(otherNi
            .setUptimeMillis(2)
            .build());
        hcHistory.record(exec2, 2L, otherHc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce2 = hcHistory.last(exec2);
        assertNotNull(otherHce2);
        assertThat(hcHistory.entries())
            .hasSize(6)
            .contains(otherHce2);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(1)
            .contains(otherHce2);
        assertEquals(1L, otherHce2.getTimestampStart());
        assertEquals(2L, otherHce2.getTimestampEnd());

        // toggle a field
        otherHc.setInfo(otherNi
            .setNativeTransportRunning(true)
            .build());
        hcHistory.record(exec2, 3L, otherHc.build());

        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce3 = hcHistory.last(exec2);
        assertNotNull(otherHce3);
        assertThat(hcHistory.entries())
            .hasSize(7)
            .contains(otherHce2)
            .contains(otherHce3);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(2)
            .contains(otherHce2)
            .contains(otherHce3);
        assertEquals(3L, otherHce3.getTimestampStart());
        assertEquals(3L, otherHce3.getTimestampEnd());

        // toggle more fields and record
        otherHc.setInfo(otherNi
            .setNativeTransportRunning(false)
            .setRpcServerRunning(true)
            .build());
        hcHistory.record(exec2, 4L, otherHc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce4 = hcHistory.last(exec2);
        assertThat(hcHistory.entries()).hasSize(8);

        otherHc.setInfo(otherNi
            .setNativeTransportRunning(true)
            .setRpcServerRunning(true)
            .build());
        hcHistory.record(exec2, 5L, otherHc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce5 = hcHistory.last(exec2);
        assertThat(hcHistory.entries()).hasSize(9);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(4)
            .contains(otherHce2)
            .contains(otherHce3)
            .contains(otherHce4)
            .contains(otherHce5);

        otherHc.setHealthy(true)
            .setMsg("msg");
        hcHistory.record(exec2, 6L, otherHc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce6 = hcHistory.last(exec2);
        assertThat(hcHistory.entries()).hasSize(10);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(5)
            .contains(otherHce2)
            .contains(otherHce3)
            .contains(otherHce4)
            .contains(otherHce5)
            .contains(otherHce6);

        otherHc.setHealthy(false)
            .setMsg("foo");
        hcHistory.record(exec2, 7L, otherHc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce7 = hcHistory.last(exec2);
        assertThat(hcHistory.entries()).hasSize(10);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(5)
            .doesNotContain(otherHce2)
            .contains(otherHce3)
            .contains(otherHce4)
            .contains(otherHce5)
            .contains(otherHce6)
            .contains(otherHce7);

        otherHc.setHealthy(true)
            .clearMsg();
        hcHistory.record(exec2, 8L, otherHc.build());
        CassandraFrameworkProtos.HealthCheckHistoryEntry otherHce8 = hcHistory.last(exec2);
        assertThat(hcHistory.entries())
            .hasSize(10)
            //
            .doesNotContain(otherHce2)
            .doesNotContain(otherHce3)
            .contains(otherHce4)
            .contains(otherHce5)
            .contains(otherHce6)
            .contains(otherHce7)
            .contains(otherHce8)
            //
            .doesNotContain(hce2)
            .doesNotContain(hce3)
            .contains(hce4)
            .contains(hce5)
            .contains(hce6)
            .contains(hce7)
            .contains(hce8);
        assertThat(hcHistory.entriesForExecutor(exec2))
            .hasSize(5)
            .doesNotContain(otherHce2)
            .doesNotContain(otherHce3)
            .contains(otherHce4)
            .contains(otherHce5)
            .contains(otherHce6)
            .contains(otherHce7)
            .contains(otherHce8);

    }

    @Test
    public void testIsSimilarEntryHealthCheckDetails() throws Exception {
        CassandraFrameworkProtos.HealthCheckDetails.Builder hc1 = CassandraFrameworkProtos.HealthCheckDetails.newBuilder();
        CassandraFrameworkProtos.HealthCheckDetails.Builder hc2 = CassandraFrameworkProtos.HealthCheckDetails.newBuilder();

        CassandraFrameworkProtos.NodeInfo.Builder ni1 = CassandraFrameworkProtos.NodeInfo.newBuilder();
        CassandraFrameworkProtos.NodeInfo.Builder ni2 = CassandraFrameworkProtos.NodeInfo.newBuilder();

        hc1.setHealthy(true);
        hc2.setHealthy(true);

        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        hc1.setInfo(ni1);
        hc2.setInfo(ni2);

        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        ni1.setUptimeMillis(1234);
        ni2.setUptimeMillis(5678);
        ni1.setClusterName("cluster");
        ni2.setClusterName("cluster");
        ni1.setNativeTransportRunning(true);
        ni2.setNativeTransportRunning(true);
        hc1.setInfo(ni1);
        hc2.setInfo(ni2);

        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        ni2.setNativeTransportRunning(false);
        hc2.setInfo(ni2);
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        ni2.setNativeTransportRunning(true);
        ni2.setClusterName("foo");
        hc2.setInfo(ni2);
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        ni2.setClusterName("cluster");
        hc2.setInfo(ni2);
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        hc2.setHealthy(false);
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        hc2.setHealthy(true);
        hc2.setMsg("msg");
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        hc2.clearMsg();
        hc1.setMsg("msg");
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

        hc1.clearMsg();
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(hc1.build(), hc2.build()));

    }

    @Test
    public void testIsSimilarEntryNodeInfo() throws Exception {
        CassandraFrameworkProtos.NodeInfo.Builder ni1 = CassandraFrameworkProtos.NodeInfo.newBuilder();
        CassandraFrameworkProtos.NodeInfo.Builder ni2 = CassandraFrameworkProtos.NodeInfo.newBuilder();

        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));
        ni1.setUptimeMillis(1234);
        ni2.setUptimeMillis(5678);
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni1.setClusterName("cluster");
        ni2.setClusterName("cluster");
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni1.setNativeTransportRunning(true);
        ni2.setNativeTransportRunning(true);
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni2.setNativeTransportRunning(false);
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni2.setNativeTransportRunning(true);
        ni2.setClusterName("foo");
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni2.setNativeTransportRunning(true);
        ni2.setClusterName("cluster");
        assertTrue(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni1.setDataCenter("dc");
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));

        ni1.clearDataCenter();
        ni2.setRack("rac");
        assertFalse(PersistedCassandraClusterHealthCheckHistory.isSimilarEntry(ni1.build(), ni2.build()));
    }

    @Test
    public void testObjEquals() throws Exception {
        assertTrue(PersistedCassandraClusterHealthCheckHistory.objEquals(null, null));
        assertFalse(PersistedCassandraClusterHealthCheckHistory.objEquals(null, ""));
        assertFalse(PersistedCassandraClusterHealthCheckHistory.objEquals("", null));
        assertTrue(PersistedCassandraClusterHealthCheckHistory.objEquals("1", "1"));
        assertTrue(PersistedCassandraClusterHealthCheckHistory.objEquals(1, 1));
        assertFalse(PersistedCassandraClusterHealthCheckHistory.objEquals("1", "2"));
        assertFalse(PersistedCassandraClusterHealthCheckHistory.objEquals(1, 2));
    }

    @Test
    public void testAllOscillatingOutOfOrder() throws Exception {
        State state = new InMemoryState();
        PersistedCassandraClusterHealthCheckHistory hcHistory = new PersistedCassandraClusterHealthCheckHistory(state);

        hcHistory.record("abc", 10, unhealthy());
        hcHistory.record("abc", 11,   healthy());
        hcHistory.record("abc", 13, unhealthy());
        hcHistory.record("abc", 16, unhealthy());
        hcHistory.record("abc", 14,   healthy());
        hcHistory.record("abc", 19, unhealthy());
        hcHistory.record("abc", 15,   healthy());
        hcHistory.record("abc", 12,   healthy());
        hcHistory.record("abc", 25, unhealthy());
        hcHistory.record("abc", 17,   healthy());
        hcHistory.record("abc", 20,   healthy());
        hcHistory.record("abc", 21,   healthy());
        hcHistory.record("abc", 22, unhealthy());
        hcHistory.record("abc", 24,   healthy());
        hcHistory.record("abc", 18,   healthy());
        hcHistory.record("abc", 23,   healthy());

        final CassandraFrameworkProtos.CassandraClusterHealthCheckHistory history = hcHistory.get();
        final List<CassandraFrameworkProtos.HealthCheckHistoryEntry> list = history.getEntriesList();

        CassandraFrameworkProtos.HealthCheckHistoryEntry prev = null;
        for (final CassandraFrameworkProtos.HealthCheckHistoryEntry entry : list) {
            if (prev != null) {
                assertThat(entry.getTimestampStart()).isLessThanOrEqualTo(entry.getTimestampEnd());
                assertThat(prev.getTimestampEnd()).isLessThanOrEqualTo(entry.getTimestampStart());
            }
            prev = entry;
        }
    }

    static CassandraFrameworkProtos.HealthCheckDetails healthy() {
        return healthCheckDetails(true);
    }
    static CassandraFrameworkProtos.HealthCheckDetails unhealthy() {
        return healthCheckDetails(false);
    }

    static CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails(final boolean healthy) {
        return CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
            .setHealthy(healthy)
            .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                .setClusterName("cluster")
                .setDataCenter("dc1")
                .setUptimeMillis(1).build()).build();
    }
}