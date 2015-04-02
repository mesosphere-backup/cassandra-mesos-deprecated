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

import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.*;

public class NodeTargetStateTest extends AbstractCassandraSchedulerTest {

    @Test
    public void testNonExistant() throws InvalidProtocolBufferException {
        threeNodeCluster();

        assertNull(cluster.nodeStop("42"));
        assertNull(cluster.nodeRun("42"));
        assertNull(cluster.nodeTerminate("42"));
        assertNull(cluster.nodeRestart("42"));
    }

    @Test
    public void testNodeTargetStateShutdownAndRun() throws Exception {

        threeNodeCluster();

        noopOnOffer(slaves[0], 3);
        noopOnOffer(slaves[1], 3);
        noopOnOffer(slaves[2], 3);

        assertNull(cluster.nodeStop("foo bar baz"));

        CassandraFrameworkProtos.CassandraNode node1 = cluster.nodeStop(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.STOP, node1.getTargetRunState());

        CassandraFrameworkProtos.CassandraNodeTask taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(taskForNode);

        // verify that kill-task is launched
        killTask(slaves[0], taskForNode.getTaskId());
        // must not repeat CASSANDRA_SERVER_SHUTDOWN since it's already launched
        noopOnOffer(slaves[0], 3, true);

        // re-check that server-task is still present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // simulate server-task has finished
        executorTaskFinished(executorServer[0]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());

        // check that server-task is no longer present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));


        // now start node

        node1 = cluster.nodeRun(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN, node1.getTargetRunState());

        // verify that CASSANDRA_SERVER_RUN task is launched
        launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
        // must not repeat CASSANDRA_SERVER_RUN since it's already launched
        noopOnOffer(slaves[0], 3);

        // check that server-task is present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN, node1.getTargetRunState());
        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // simulate server-task is running
        sendHealthCheckResult(executorMetadata[0], healthCheckDetailsSuccess("NORMAL", true));


        // TERMINATE

        node1 = cluster.nodeTerminate(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE, node1.getTargetRunState());

        taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(taskForNode);

        // verify that kill-task is launched
        killTask(slaves[0], taskForNode.getTaskId());
        // must not repeat CASSANDRA_SERVER_SHUTDOWN since it's already launched
        noopOnOffer(slaves[0], 3, true);

        // re-check that server-task is still present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // simulate server-task has finished
        executorTaskFinished(executorServer[0]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());

        // check that server-task is no longer present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // try RUN

        node1 = cluster.nodeRun(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE, node1.getTargetRunState());

        // try STOP

        node1 = cluster.nodeStop(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE, node1.getTargetRunState());

        // try RESTART

        node1 = cluster.nodeRestart(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE, node1.getTargetRunState());


    }

    @Test
    public void testNodeTargetStateRestart() throws Exception {

        threeNodeCluster();

        noopOnOffer(slaves[0], 3);
        noopOnOffer(slaves[1], 3);
        noopOnOffer(slaves[2], 3);

        assertNull(cluster.nodeRestart("foo bar baz"));

        CassandraFrameworkProtos.CassandraNode node1 = cluster.nodeRestart(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART, node1.getTargetRunState());

        CassandraFrameworkProtos.CassandraNodeTask taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(taskForNode);

        // verify that kill-task is launched
        killTask(slaves[0], taskForNode.getTaskId());
        // must not repeat CASSANDRA_SERVER_SHUTDOWN since it's already launched
        noopOnOffer(slaves[0], 3, true);

        // re-check that server-task is still present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART, node1.getTargetRunState());
        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // simulate server-task has finished
        executorTaskFinished(executorServer[0]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());

        // check that server-task is no longer present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART, node1.getTargetRunState());
        assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));



        // verify that CASSANDRA_SERVER_RUN task is launched
        launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
        // must not repeat CASSANDRA_SERVER_RUN since it's already launched
        noopOnOffer(slaves[0], 3);

        // check that server-task is present
        node1 = cluster.findNode(slaves[0]._2);
        assertNotNull(node1);
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN, node1.getTargetRunState());
        assertNotNull(CassandraFrameworkProtosUtils.getTaskForNode(node1, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

        // simulate server-task is running
        sendHealthCheckResult(executorMetadata[0], healthCheckDetailsSuccess("NORMAL", true));

    }

    @Test
    public void testServerTaskRemove() throws InvalidProtocolBufferException {

        Protos.TaskInfo[] executorMetadata = threeNodeCluster();

        // cluster now up with 3 running nodes

        // server-task no longer running
        executorTaskError(executorMetadata[0]);

        // server-task cannot start again
        launchExecutor(slaves[0], 3);
        executorTaskRunning(executorMetadata[0]);
        launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);

    }

}
