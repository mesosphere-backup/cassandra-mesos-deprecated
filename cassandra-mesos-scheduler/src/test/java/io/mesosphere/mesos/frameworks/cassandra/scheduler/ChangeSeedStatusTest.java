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
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChangeSeedStatusTest extends AbstractCassandraSchedulerTest {

    @Test
    public void testSeedChanged() throws Exception {

        cleanState();

        assertEquals(2, clusterState.get().getSeedsToAcquire());

        // rollout slaves
        executorMetadata = new Protos.TaskInfo[slaves.length];
        executorMetadata[0] = launchExecutor(slaves[0], 1);

        try {
            cluster.setNodeSeed(cluster.findNode(slaves[0]._2), false);
            fail();
        } catch (SeedChangeException e) {
            assertEquals("Must not change seed status while initial number of seed nodes has not been acquired", e.getMessage());
        }

        assertEquals(1, clusterState.get().getSeedsToAcquire());
        executorMetadata[1] = launchExecutor(slaves[1], 2);
        assertEquals(0, clusterState.get().getSeedsToAcquire());
        executorMetadata[2] = launchExecutor(slaves[2], 3);
        assertEquals(0, clusterState.get().getSeedsToAcquire());

        threeNodeClusterPost();

        assertFalse(cluster.setNodeSeed(cluster.findNode(slaves[0]._2), true));
        assertFalse(cluster.setNodeSeed(cluster.findNode(slaves[1]._2), true));
        assertFalse(cluster.setNodeSeed(cluster.findNode(slaves[2]._2), false));

        for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.nodes()) {
            assertFalse(cassandraNode.getNeedsConfigUpdate());
        }

        noopOnOffer(slaves[0], 3);
        noopOnOffer(slaves[1], 3);
        noopOnOffer(slaves[2], 3);

        // make 3rd node a seed

        assertTrue(cluster.setNodeSeed(cluster.findNode(slaves[2]._2), true));
        for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.nodes()) {
            assertTrue(cassandraNode.getNeedsConfigUpdate());
        }

        // verify UPDATE_CONFIG tasks are launched
        launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        launchTask(slaves[1], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        launchTask(slaves[2], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        noopOnOffer(slaves[0], 3);
        noopOnOffer(slaves[1], 3);
        noopOnOffer(slaves[2], 3);

        for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.nodes()) {
            assertFalse(cassandraNode.getNeedsConfigUpdate());
        }

        // make 1st node not a seed
        assertTrue(cluster.setNodeSeed(cluster.findNode(slaves[0]._2), false));
        for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.nodes()) {
            assertTrue(cassandraNode.getNeedsConfigUpdate());
        }

        // verify UPDATE_CONFIG tasks are launched
        launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        launchTask(slaves[1], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        launchTask(slaves[2], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.UPDATE_CONFIG);
        noopOnOffer(slaves[0], 3);
        noopOnOffer(slaves[1], 3);
        noopOnOffer(slaves[2], 3);

        for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.nodes()) {
            assertFalse(cassandraNode.getNeedsConfigUpdate());
        }

        // mark two nodes as deas so that last remaining (seed) cannot be made non-seed
        sendHealthCheckResult(executorMetadata[2], healthCheckDetailsFailed());

        try {
            cluster.setNodeSeed(cluster.findNode(slaves[1]._2), false);
            fail();
        } catch (SeedChangeException e) {
            assertEquals("Must not remove the last live seed node", e.getMessage());
        }
    }

}
