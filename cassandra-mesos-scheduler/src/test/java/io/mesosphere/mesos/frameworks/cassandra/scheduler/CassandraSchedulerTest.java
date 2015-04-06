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
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import io.mesosphere.mesos.util.ProtoUtils;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

public class CassandraSchedulerTest extends AbstractCassandraSchedulerTest {
    @Test
    public void testReregistration() throws Exception {
        threeNodeCluster();

        List<CassandraFrameworkProtos.CassandraNode> nodes = cluster.getClusterState().nodes();
        assertThat(nodes).hasSize(3);
        for (CassandraFrameworkProtos.CassandraNode node : nodes) {
            assertNotNull(node);
            CassandraFrameworkProtos.CassandraNodeExecutor exec = node.getCassandraNodeExecutor();
            assertNotNull(exec);
            assertThat(nodes).isNotEmpty();
            for (CassandraFrameworkProtos.FileDownload down : exec.getDownloadList()) {
                assertThat(down.getDownloadUrl()).startsWith("http://127.0.0.1:65535/");
            }
        }

        scheduler.disconnected(driver);

        //

        cluster = new CassandraCluster(new SystemClock(),
            "http://127.42.42.42:42",
            new ExecutorCounter(state, 0L),
            new PersistedCassandraClusterState(state, 3, 2),
            new PersistedCassandraClusterHealthCheckHistory(state),
            new PersistedCassandraClusterJobs(state),
            configuration);
        clusterState = cluster.getClusterState();
        scheduler = new CassandraScheduler(configuration, cluster);
        driver = new MockSchedulerDriver(scheduler);

        driver.callReRegistered();

        nodes = cluster.getClusterState().nodes();
        assertThat(nodes).hasSize(3);
        for (CassandraFrameworkProtos.CassandraNode node : nodes) {
            assertNotNull(node);
            CassandraFrameworkProtos.CassandraNodeExecutor exec = node.getCassandraNodeExecutor();
            assertNotNull(exec);
            assertThat(nodes).isNotEmpty();
            for (CassandraFrameworkProtos.FileDownload down : exec.getDownloadList()) {
                assertThat(down.getDownloadUrl()).startsWith("http://127.42.42.42:42/");
            }
        }

    }

    @Test
    public void testIsLiveNode() throws Exception {
        cleanState();

        CassandraFrameworkProtos.CassandraNode.Builder node = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setHostname("bart")
            .setIp("1.2.3.4")
            .setSeed(false)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setIp("1.2.3.4").setJmxPort(7199));

        assertFalse(node.hasCassandraNodeExecutor());
        assertFalse(cluster.isLiveNode(node.build()));

        node.setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
            .addCommand("cmd")
            .setResources(someResources())
            .setExecutorId("exec")
            .setSource("src"));

        assertTrue(node.hasCassandraNodeExecutor());
        assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));
        assertFalse(cluster.isLiveNode(node.build()));

        node.addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
            .setResources(someResources())
            .setTaskId("task")
            .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node.build(), CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));
        assertFalse(cluster.isLiveNode(node.build()));

        CassandraFrameworkProtos.HealthCheckDetails.Builder hcd = CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
            .setHealthy(false);
        cluster.recordHealthCheck("exec", hcd.build());
        assertNotNull(cluster.lastHealthCheck("exec"));
        assertFalse(cluster.isLiveNode(node.build()));

        hcd.setHealthy(true);
        cluster.recordHealthCheck("exec", hcd.build());
        assertNotNull(cluster.lastHealthCheck("exec"));
        assertFalse(cluster.isLiveNode(node.build()));

        CassandraFrameworkProtos.NodeInfo.Builder ni = CassandraFrameworkProtos.NodeInfo.newBuilder();
        hcd.setInfo(ni);
        cluster.recordHealthCheck("exec", hcd.build());
        assertFalse(cluster.isLiveNode(node.build()));

        ni.setRpcServerRunning(true);
        hcd.setInfo(ni);
        cluster.recordHealthCheck("exec", hcd.build());
        assertFalse(cluster.isLiveNode(node.build()));

        ni.setNativeTransportRunning(true);
        hcd.setInfo(ni);
        cluster.recordHealthCheck("exec", hcd.build());
        assertTrue(cluster.isLiveNode(node.build()));
    }

    @Test
    public void testGetPortMapping() {
        CassandraFrameworkProtos.CassandraFrameworkConfiguration config = CassandraFrameworkProtos.CassandraFrameworkConfiguration.newBuilder()
            .setDefaultConfigRole(CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
                .setResources(resources(1, 2048, 2048)))
            .setFrameworkName("a name")
            .setHealthCheckIntervalSeconds(10)
            .setBootstrapGraceTimeSeconds(10)
            .build();

        try {
            CassandraCluster.getPortMapping(config, "foobar");
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }

        assertEquals(7199, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_JMX));
        assertEquals(9042, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_NATIVE));
        assertEquals(9160, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_RPC));
        assertEquals(7000, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE));
        assertEquals(7001, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE_SSL));

        config = CassandraFrameworkProtos.CassandraFrameworkConfiguration.newBuilder()
            .setDefaultConfigRole(CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
                .setResources(resources(1, 2048, 2048)))
            .setFrameworkName("a name")
            .setHealthCheckIntervalSeconds(10)
            .setBootstrapGraceTimeSeconds(10)
            .addPortMapping(CassandraFrameworkProtos.PortMapping.newBuilder()
                .setName(CassandraCluster.PORT_JMX)
                .setPort(1))
            .addPortMapping(CassandraFrameworkProtos.PortMapping.newBuilder()
                .setName(CassandraCluster.PORT_NATIVE)
                .setPort(2))
            .addPortMapping(CassandraFrameworkProtos.PortMapping.newBuilder()
                .setName(CassandraCluster.PORT_RPC)
                .setPort(3))
            .addPortMapping(CassandraFrameworkProtos.PortMapping.newBuilder()
                .setName(CassandraCluster.PORT_STORAGE)
                .setPort(4))
            .addPortMapping(CassandraFrameworkProtos.PortMapping.newBuilder()
                .setName(CassandraCluster.PORT_STORAGE_SSL)
                .setPort(5))
            .build();

        assertEquals(1, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_JMX));
        assertEquals(2, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_NATIVE));
        assertEquals(3, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_RPC));
        assertEquals(4, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE));
        assertEquals(5, CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE_SSL));
    }

    @Test
    public void testMetadataForExecutor() {
        cleanState();

        clusterState.executorMetadata(Arrays.asList(
            CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId("exec1")
                .setIp("1.1.1.1")
                .setWorkdir("/foo")
                .build(),
            CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId("exec2")
                .setIp("2.2.2.2")
                .setWorkdir("/bar")
                .build()
        ));

        assertNull(cluster.metadataForExecutor("none"));

        CassandraFrameworkProtos.ExecutorMetadata em = cluster.metadataForExecutor("exec1");
        assertNotNull(em);
        assertEquals("/foo", em.getWorkdir());

        em = cluster.metadataForExecutor("exec2");
        assertNotNull(em);
        assertEquals("/bar", em.getWorkdir());

        cluster.removeExecutor("exec1");

        assertNull(cluster.metadataForExecutor("none"));
        assertNull(cluster.metadataForExecutor("exec1"));

        em = cluster.metadataForExecutor("exec2");
        assertNotNull(em);
        assertEquals("/bar", em.getWorkdir());
    }

    @Test
    public void testNodeLogFiles() {
        cleanState();

        CassandraFrameworkProtos.CassandraNode node1 = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setIp("1.1.1.1")
            .setHostname("host1")
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("1.1.1.1"))
            .setSeed(true)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                .setResources(someResources())
                .setSource("src")
                .setExecutorId("host1"))
            .build();
        CassandraFrameworkProtos.CassandraNode node2 = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setIp("2.2.2.2")
            .setHostname("host2")
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("2.2.2.2"))
            .setSeed(true)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                .setResources(someResources())
                .setSource("src")
                .setExecutorId("host2"))
            .build();

        clusterState.nodes(Arrays.asList(
            node1,
            node2
        ));

        clusterState.executorMetadata(Arrays.asList(
            CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId("host1")
                .setIp("1.1.1.1")
                .setWorkdir("/foo")
                .build(),
            CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId("host2")
                .setIp("2.2.2.2")
                .setWorkdir("/bar")
                .build()
        ));

        assertThat(cluster.getNodeLogFiles(node1))
            .hasSize(2)
            .contains(
                "/foo/executor.log",
                "/foo/apache-cassandra-2.1.4/logs/system.log");

        assertThat(cluster.getNodeLogFiles(node2))
            .hasSize(2)
            .contains(
                "/bar/executor.log",
                "/bar/apache-cassandra-2.1.4/logs/system.log");
    }

    @Test
    public void testServerProcessPid() {
        cleanState();

        CassandraFrameworkProtos.CassandraNode node1 = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setIp("1.1.1.1")
            .setHostname("host1")
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("1.1.1.1"))
            .setSeed(true)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                .setResources(someResources())
                .setSource("src")
                .setExecutorId("host1"))
            .build();
        CassandraFrameworkProtos.CassandraNode node2 = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setIp("2.2.2.2")
            .setHostname("host2")
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("2.2.2.2"))
            .setSeed(true)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                .setResources(someResources())
                .setSource("src")
                .setExecutorId("host2"))
            .build();

        clusterState.nodes(Arrays.asList(
            node1,
            node2
        ));

        cluster.updateCassandraProcess(Protos.ExecutorID.newBuilder().setValue("host1").build(),
            CassandraFrameworkProtos.CassandraServerRunMetadata.newBuilder()
                .setPid(123).build());

        CassandraFrameworkProtos.CassandraNode node = cluster.findNode("1.1.1.1");
        assertNotNull(node);
        assertTrue(node.hasCassandraDaemonPid());
        assertEquals(123, node.getCassandraDaemonPid());

        node = cluster.findNode("2.2.2.2");
        assertNotNull(node);
        assertFalse(node.hasCassandraDaemonPid());

        cluster.updateCassandraProcess(Protos.ExecutorID.newBuilder().setValue("host2").build(),
            CassandraFrameworkProtos.CassandraServerRunMetadata.newBuilder()
                .setPid(456).build());

        node = cluster.findNode("host2");
        assertNotNull(node);
        assertTrue(node.hasCassandraDaemonPid());
        assertEquals(456, node.getCassandraDaemonPid());

        cluster.updateCassandraProcess(Protos.ExecutorID.newBuilder().setValue("42-is-not-true").build(),
            CassandraFrameworkProtos.CassandraServerRunMetadata.newBuilder()
                .setPid(999).build());
    }

    @Test
    public void testNodeAndExecutorForTask() {
        cleanState();

        clusterState.nodes(Arrays.asList(
            CassandraFrameworkProtos.CassandraNode.newBuilder()
                .setIp("1.1.1.1")
                .setHostname("host1")
                .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("1.1.1.1"))
                .setSeed(true)
                .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
                .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                    .setTaskId("host1.task1")
                    .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER)
                    .setResources(someResources()))
                .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                    .setTaskId("host1.task2")
                    .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.METADATA)
                    .setResources(someResources()))
                .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                    .setResources(someResources())
                    .setSource("src")
                    .setExecutorId("host1"))
                .build(),
            CassandraFrameworkProtos.CassandraNode.newBuilder()
                .setIp("2.2.2.2")
                .setHostname("host2")
                .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setJmxPort(1).setIp("2.2.2.2"))
                .setSeed(true)
                .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
                .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                    .setTaskId("host2.task1")
                    .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER)
                    .setResources(someResources()))
                .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                    .setTaskId("host2.task2")
                    .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.METADATA)
                    .setResources(someResources()))
                .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                    .setResources(someResources())
                    .setSource("src")
                    .setExecutorId("host2"))
                .build()
        ));

        assertFalse(cluster.getNodeForTask("foo").isPresent());
        assertFalse(cluster.getExecutorIdForTask("foo").isPresent());

        assertTrue(cluster.getNodeForTask("host1.task1").isPresent());
        assertTrue(cluster.getExecutorIdForTask("host2.task1").isPresent());

        assertEquals("1.1.1.1", cluster.getNodeForTask("host1.task1").get().getIp());
        assertEquals("host1", cluster.getExecutorIdForTask("host1.task1").get());

        assertEquals("2.2.2.2", cluster.getNodeForTask("host2.task2").get().getIp());
        assertEquals("host2", cluster.getExecutorIdForTask("host2.task2").get());

        //

        cluster.removeTask("host2.task1", Protos.TaskStatus.newBuilder()
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue("host2"))
            .setMessage("msg")
            .setReason(Protos.TaskStatus.Reason.REASON_TASK_UNKNOWN)
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave2"))
            .setTaskId(Protos.TaskID.newBuilder().setValue("host2.task1"))
            .setTimestamp(1d)
            .setState(Protos.TaskState.TASK_FAILED)
            .build());

        assertFalse(cluster.getNodeForTask("host2.task1").isPresent());
        assertFalse(cluster.getExecutorIdForTask("host2.task1").isPresent());

        CassandraFrameworkProtos.CassandraNode node = cluster.findNode("host1");
        assertNotNull(node);
        assertFalse(node.getTasksList().isEmpty());

        assertTrue(cluster.getNodeForTask("host1.task1").isPresent());
        assertTrue(cluster.getExecutorIdForTask("host1.task1").isPresent());

        cluster.removeExecutor("host1");

        node = cluster.findNode("host1");
        assertNotNull(node);
        assertTrue(node.getTasksList().isEmpty());

        assertFalse(cluster.getNodeForTask("host1.task1").isPresent());
        assertFalse(cluster.getExecutorIdForTask("host1.task1").isPresent());
    }

    @Test
    public void testHasResources() {
        Protos.Offer offer = Protos.Offer.newBuilder()
            .setHostname("host1")
            .setId(Protos.OfferID.newBuilder().setValue("offer"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frw1"))
            .build();

        List<String> errs = CassandraCluster.hasResources(offer,
            resources(0, 0, 0), Collections.<String, Long>emptyMap(), "*");
        assertNotNull(errs);
        assertThat(errs)
            .isEmpty();

        Locale.setDefault(Locale.ENGLISH); // required for correct float comparison!

        errs = CassandraCluster.hasResources(offer,
            resources(1, 2, 3), new HashMap<String, Long>() {{
                put("port1", 1L);
                put("port2", 2L);
                put("port3", 3L);
            }}, "ROLE");
        assertNotNull(errs);
        assertThat(errs)
            .hasSize(6)
            .contains(
                "Not enough cpu resources for role ROLE. Required 1.0 only 0.0 available",
                "Not enough mem resources for role ROLE. Required 2 only 0 available",
                "Not enough disk resources for role ROLE. Required 3 only 0 available",
                "Unavailable port 1(port1) for role ROLE. 0 other ports available",
                "Unavailable port 2(port2) for role ROLE. 0 other ports available",
                "Unavailable port 3(port3) for role ROLE. 0 other ports available"
            );

        offer = Protos.Offer.newBuilder()
            .setHostname("host1")
            .setId(Protos.OfferID.newBuilder().setValue("offer"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frw1"))
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)))
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)))
            .addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)))
            .addResources(Protos.Resource.newBuilder()
                .setName("ports")
                .setRole("*")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder()
                    .addRange(Protos.Value.Range.newBuilder().setBegin(7000).setEnd(10000)))
            )
            .build();

        errs = CassandraCluster.hasResources(offer,
            resources(8, 8192, 8192), new HashMap<String, Long>() {{
                put("port1", 7000L);
                put("port2", 7002L);
                put("port3", 10000L);
            }}, "*");
        assertNotNull(errs);
        assertThat(errs)
            .isEmpty();

        offer = Protos.Offer.newBuilder()
            .setHostname("host1")
            .setId(Protos.OfferID.newBuilder().setValue("offer"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frw1"))
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setRole("BAZ")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)))
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setRole("BAZ")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)))
            .addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setRole("BAZ")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)))
            .addResources(Protos.Resource.newBuilder()
                .setName("ports")
                .setRole("BAZ")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder()
                    .addRange(Protos.Value.Range.newBuilder().setBegin(7000).setEnd(10000)))
            )
            .build();

        errs = CassandraCluster.hasResources(offer,
            resources(8, 8192, 8192), new HashMap<String, Long>() {{
                put("port1", 7000L);
                put("port2", 7002L);
                put("port3", 10000L);
            }}, "BAZ");
        assertNotNull(errs);
        assertThat(errs)
            .isEmpty();

        errs = CassandraCluster.hasResources(offer,
            resources(8, 8192, 8192), new HashMap<String, Long>() {{
                put("port1", 7000L);
                put("port2", 7002L);
                put("port3", 10000L);
            }}, "FOO_BAR");
        assertNotNull(errs);
        assertThat(errs)
            .hasSize(6)
            .contains(
                "Not enough cpu resources for role FOO_BAR. Required 8.0 only 0.0 available",
                "Not enough mem resources for role FOO_BAR. Required 8192 only 0 available",
                "Not enough disk resources for role FOO_BAR. Required 8192 only 0 available",
                "Unavailable port 7000(port1) for role FOO_BAR. 0 other ports available",
                "Unavailable port 7002(port2) for role FOO_BAR. 0 other ports available",
                "Unavailable port 10000(port3) for role FOO_BAR. 0 other ports available"
            );

    }

    @Test
    public void testHasResource_floatingPointPrecision() throws Exception {
        Protos.Offer offer = Protos.Offer.newBuilder()
                .setHostname("host1")
                .setId(Protos.OfferID.newBuilder().setValue("offer"))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave"))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frw1"))
                .addResources(ProtoUtils.cpu(0.09999999999999981, "*"))
                .addResources(ProtoUtils.mem(1024, "*"))
                .addResources(ProtoUtils.disk(1024, "*"))
                .build();
        final CassandraFrameworkProtos.TaskResources resources = resources(
                0.1,
                500,
                500
        );

        assertThat(CassandraCluster.hasResources(offer, resources, Collections.<String, Long>emptyMap(), "*")).contains(
                "Not enough cpu resources for role *. Required 0.1 only 0.09999999999999981 available"
        );
    }

    @Test
    public void testGetTaskName_nameSpecified() throws Exception {
        assertThat(CassandraScheduler.getTaskName("name", "task")).isEqualTo("name");
    }

    @Test
    public void testGetTaskName_nameNull() throws Exception {
        assertThat(CassandraScheduler.getTaskName(null, "task")).isEqualTo("task");
    }

    @Test
    public void testGetTaskName_nameEmpty() throws Exception {
        assertThat(CassandraScheduler.getTaskName("", "task")).isEqualTo("task");
    }

    @Test
    public void testGetTaskName_nameEmptyAfterTrim() throws Exception {
        assertThat(CassandraScheduler.getTaskName("   \t", "task")).isEqualTo("task");
    }
}
