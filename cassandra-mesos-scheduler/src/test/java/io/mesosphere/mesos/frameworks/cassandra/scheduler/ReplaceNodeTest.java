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
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class ReplaceNodeTest extends AbstractCassandraSchedulerTest {

    @Test
    public void testReplaceNodePrerequirements() throws Exception {
        cleanState();

        try {
            // non-existing node
            cluster.replaceNode("foobar");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().startsWith("Non-existing node "));
        }

        CassandraFrameworkProtos.CassandraNode.Builder node = CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setHostname("bart")
            .setIp("1.2.3.4")
            .setSeed(true)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder().setIp("1.2.3.4").setJmxPort(7199))
            .setCassandraNodeExecutor(CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder()
                .addCommand("cmd")
                .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                    .setCpuCores(1)
                    .setDiskMb(1)
                    .setMemMb(1))
                .setExecutorId("exec")
                .setSource("src"))
            .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                    .setCpuCores(1)
                    .setDiskMb(1)
                    .setMemMb(1))
                .setTaskId("task")
                .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));
        cluster.getClusterState().addOrSetNode(node.build());
        CassandraFrameworkProtos.HealthCheckDetails.Builder hcd = CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
            .setHealthy(true)
            .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                .setRpcServerRunning(true)
                .setNativeTransportRunning(true));
        cluster.recordHealthCheck("exec", hcd.build());
        assertTrue(cluster.isLiveNode(cluster.findNode("exec")));
        assertTrue(cluster.isLiveNode(cluster.findNode("bart")));
        assertTrue(cluster.isLiveNode(cluster.findNode("1.2.3.4")));

        try {
            cluster.replaceNode("bart");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().endsWith("to replace is a seed node"));
        }

        cluster.getClusterState().addOrSetNode(node
            .setSeed(false)
            .build());

        try {
            cluster.replaceNode("bart");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().startsWith("Cannot replace live node "));
        }

        hcd.setHealthy(false)
            .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                .setRpcServerRunning(false)
                .setNativeTransportRunning(false));
        cluster.recordHealthCheck("exec", hcd.build());

        try {
            cluster.replaceNode("bart");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().startsWith("Cannot replace non-terminated node "));
        }

        cluster.getClusterState().addOrSetNode(node
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE)
            .build());

        try {
            cluster.replaceNode("1.2.3.4");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().endsWith(" to replace has active tasks"));
        }

        cluster.getClusterState().addOrSetNode(node
            .clearTasks()
            .build());

        cluster.replaceNode("exec");

        try {
            cluster.replaceNode("bart");
            fail();
        } catch (ReplaceNodePreconditionFailed e) {
            assertTrue(e.getMessage().endsWith(" already in replace-list"));
        }

    }

    @Test
    public void testNodeReplace() throws Exception {

        threeNodeCluster();

        CassandraFrameworkProtos.CassandraNode node3 = cluster.nodeTerminate(slaves[2]._2);
        assertNotNull(node3);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE, node3.getTargetRunState());

        CassandraFrameworkProtos.CassandraNodeTask taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node3, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(taskForNode);

        // verify that kill-task is launched
        killTask(slaves[2], taskForNode.getTaskId());
        // must not repeat kill-task since it's already launched
        noopOnOffer(slaves[2], 3, true);

        // simulate server-task has finished
        executorTaskFinished(executorServer[2]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());

        killTask(slaves[2], node3.getCassandraNodeExecutor().getExecutorId());
        // must not repeat kill-task since it's already launched
        noopOnOffer(slaves[2], 3, true);

        try {
            cluster.replaceNode(slaves[2]._2);
            fail();
        } catch (ReplaceNodePreconditionFailed ignored) {
            // ignored
        }

        // simulate server-task has finished
        executorTaskFinished(executorServer[2]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());

        try {
            cluster.replaceNode(slaves[2]._2);
            fail();
        } catch (ReplaceNodePreconditionFailed ignored) {
            // ignored
        }

        // simulate executor has finished
        executorTaskFinished(executorMetadata[2], CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());


        // replace the node

        node3 = cluster.replaceNode(slaves[2]._2);
        assertNotNull(node3);

        assertEquals(1, cluster.getClusterState().get().getReplaceNodeIpsCount());

        assertEquals(3, cluster.getClusterState().get().getNodesCount());

        // add 4th node (as replacement)

        startFourthNode();

        assertThat(cluster.getClusterState().get().getReplaceNodeIpsList()).isEmpty();
        assertEquals(4, cluster.getClusterState().get().getNodesCount());

        List<String> args = executorServer[3]._2.getCassandraServerRunTask().getCommandList();
        assertThat(args).contains("-Dcassandra.replace_address=" + slaves[2]._2);

        node3 = cluster.findNode(slaves[3]._2);
        assertNotNull(node3);
        assertTrue(node3.hasReplacementForIp());

        fourthNodeRunning();

        // replaced node should have been removed from our nodes list
        assertEquals(3, cluster.getClusterState().get().getNodesCount());

        assertNull(cluster.findNode(slaves[2]._2));

        node3 = cluster.findNode(slaves[3]._2);
        assertNotNull(node3);
        assertFalse(node3.hasReplacementForIp());
    }

}
