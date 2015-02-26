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

import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class CassandraSchedulerTest extends AbstractSchedulerTest{
    CassandraScheduler scheduler;
    MockSchedulerDriver driver;

    @Test
    public void testLaunchNewCluster() {
        cleanState();

        driver.callRegistered(frameworkId);

        // rollout slave #1

        Protos.TaskInfo executorMetadata1 = launchExecutor(slave1, 1);

        // next offer must return nothing for the same slave !

        noopOnOffer(slave1, 1);

        //
        // at this point the executor for slave #1 is known but the server must not be started because we can't fulfil
        // the seed-node-count requirement
        //

        executorTaskRunning(executorMetadata1);
        noopOnOffer(slave1, 1);
        assertEquals(Collections.singletonList("127.1.1.1"), cluster.getSeedNodes());
        assertEquals(Integer.valueOf(1), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(1), clusterState.nodeCounts()._2);

        // rollout slave #2

        Protos.TaskInfo executorMetadata2 = launchExecutor(slave2, 2);

        // rollout slave #3

        Protos.TaskInfo executorMetadata3 = launchExecutor(slave3, 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(slave2, 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(slave3, 3);

        //
        // at this point all three slave have got metadata tasks
        //

        noopOnOffer(slave2, 3);
        noopOnOffer(slave3, 3);

        assertEquals(Integer.valueOf(3), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(2), clusterState.nodeCounts()._2);

        executorTaskRunning(executorMetadata2);

        assertEquals(Integer.valueOf(3), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(2), clusterState.nodeCounts()._2);

        //
        // now there are enough executor metadata to start the seed nodes - but not the non-seed nodes
        //

        // node must not start (it's not a seed node and no seed node is running)
        noopOnOffer(slave3, 3);

        launchServer(slave1);
        launchServer(slave2);
        // still - not able to start node #3
        noopOnOffer(slave3, 3);

        //
        // executor for node #3 started
        //

        executorTaskRunning(executorMetadata3);
        // still - not able to start node #3
        noopOnOffer(slave3, 3);

        //
        // simulate some health check states
        //

        sendHealthCheckResult(executorMetadata1, healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata1.getExecutor().getExecutorId().getValue()).getDetails().getHealthy());
        sendHealthCheckResult(executorMetadata2, healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata2.getExecutor().getExecutorId().getValue()).getDetails().getHealthy());
        // still - not able to start node #3
        noopOnOffer(slave3, 3);

        sendHealthCheckResult(executorMetadata1, healthCheckDetailsSuccess("JOINING", false));
        assertTrue(cluster.lastHealthCheck(executorMetadata1.getExecutor().getExecutorId().getValue()).getDetails().getHealthy());
        assertEquals("JOINING", cluster.lastHealthCheck(executorMetadata1.getExecutor().getExecutorId().getValue()).getDetails().getInfo().getOperationMode());
        sendHealthCheckResult(executorMetadata2, healthCheckDetailsFailed());
        // still - not able to start node #3
        noopOnOffer(slave3, 3);

        //
        // one seed has started up
        //

        sendHealthCheckResult(executorMetadata1, healthCheckDetailsSuccess("NORMAL", true));
        assertTrue(cluster.lastHealthCheck(executorMetadata1.getExecutor().getExecutorId().getValue()).getDetails().getHealthy());
        assertEquals("NORMAL", cluster.lastHealthCheck(executorMetadata1.getExecutor().getExecutorId().getValue()).getDetails().getInfo().getOperationMode());
        sendHealthCheckResult(executorMetadata2, healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata2.getExecutor().getExecutorId().getValue()).getDetails().getHealthy());
        // node#3 can start now
        launchServer(slave3);

    }

    @Test
    public void testServerTaskRemove() {

        cleanState();

        // rollout slaves
        Protos.TaskInfo executorMetadata1 = launchExecutor(slave1, 1);
        Protos.TaskInfo executorMetadata2 = launchExecutor(slave2, 2);
        Protos.TaskInfo executorMetadata3 = launchExecutor(slave3, 3);

        executorTaskRunning(executorMetadata1);
        executorTaskRunning(executorMetadata2);
        executorTaskRunning(executorMetadata3);

        // launch servers

        launchServer(slave1);
        launchServer(slave2);

        sendHealthCheckResult(executorMetadata1, healthCheckDetailsSuccess("NORMAL", true));
        sendHealthCheckResult(executorMetadata2, healthCheckDetailsSuccess("NORMAL", true));

        launchServer(slave3);

        sendHealthCheckResult(executorMetadata3, healthCheckDetailsSuccess("NORMAL", true));

        // cluster now up with 3 running nodes

        // server-task no longer running
        executorTaskError(executorMetadata1);

        // server-task cannot start again
        launchServer(slave1);
    }

    private void executorTaskError(Protos.TaskInfo taskInfo) {
        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
                .setExecutorId(taskInfo.getExecutor().getExecutorId())
                .setHealthy(true)
                .setSlaveId(taskInfo.getSlaveId())
                .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
                .setTaskId(taskInfo.getTaskId())
                .setTimestamp(System.currentTimeMillis())
                .setState(Protos.TaskState.TASK_ERROR)
                .build());
    }

    private void executorTaskRunning(Protos.TaskInfo taskInfo) {
        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
                .setExecutorId(taskInfo.getExecutor().getExecutorId())
                .setHealthy(true)
                .setSlaveId(taskInfo.getSlaveId())
                .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
                .setTaskId(taskInfo.getTaskId())
                .setTimestamp(System.currentTimeMillis())
                .setState(Protos.TaskState.TASK_RUNNING)
                .setData(CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.EXECUTOR_METADATA)
                        .setExecutorMetadata(CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                                .setExecutorId(taskInfo.getExecutor().getExecutorId().getValue())
                                .setIp("NO_IP!!!"))
                        .build().toByteString())
                .build());
    }

    private void sendHealthCheckResult(Protos.TaskInfo taskInfo, CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
        scheduler.frameworkMessage(driver, taskInfo.getExecutor().getExecutorId(), taskInfo.getSlaveId(),
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                .setHealthCheckDetails(healthCheckDetails).build().toByteArray());
    }

    private Protos.TaskInfo launchExecutor(Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(driver.declinedOffers().isEmpty());

        assertEquals(nodeCount, cluster.getClusterState().getNodesCount());
        assertEquals(1, launchTasks._2.size());

        return launchTasks._2.iterator().next();
    }

    private Protos.TaskInfo launchServer(Tuple2<Protos.SlaveID, String> slave) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(driver.declinedOffers().isEmpty());

        assertEquals(1, launchTasks._2.size());

        return launchTasks._2.iterator().next();
    }

    private void noopOnOffer(Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        List<Protos.OfferID> decl = driver.declinedOffers();
        assertEquals(1, decl.size());
        assertEquals(offer.getId(), decl.get(0));

        assertEquals(nodeCount, cluster.getClusterState().getNodesCount());
        assertEquals(0, launchTasks._2.size());
    }

    protected void cleanState() {
        super.cleanState();

        scheduler = new CassandraScheduler(configuration, cluster);

        driver = new MockSchedulerDriver(scheduler);
    }

}
