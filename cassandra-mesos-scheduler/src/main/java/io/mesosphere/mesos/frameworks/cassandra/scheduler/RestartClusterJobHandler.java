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

import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RestartClusterJobHandler extends ClusterJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestartClusterJobHandler.class);

    private static final long RESTART_NODE_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);

    public RestartClusterJobHandler(CassandraCluster cluster, PersistedCassandraClusterJobs jobsState) {
        super(cluster, jobsState);
    }

    @Override
    public void handleTaskOffer(CassandraFrameworkProtos.ClusterJobStatus currentJob, String executorId, Optional<CassandraFrameworkProtos.CassandraNode> nodeForExecutorId, TasksForOffer tasksForOffer) {
        if (currentJob.hasCurrentNode()) {
            CassandraFrameworkProtos.CassandraNode node = nodeForExecutorId.get();

            CassandraFrameworkProtos.NodeJobStatus nodeJobStatus = currentJob.getCurrentNode();
            if (executorId.equals(nodeJobStatus.getExecutorId())) {
                switch (nodeForExecutorId.get().getTargetRunState()) {
                    case RUN:
                        // node went into RUN state

                        CassandraFrameworkProtos.HealthCheckHistoryEntry lastHC = cluster.lastHealthCheck(executorId);

                        if (CassandraFrameworkProtosUtils.getTaskForNode(node, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER) != null &&
                            lastHC.hasTimestampEnd() &&
                            lastHC.getTimestampEnd() > nodeJobStatus.getStartedTimestamp() &&
                            cluster.isLiveNode(lastHC)) {

                            // node is live - we can continue with the next node
                            LOGGER.info("Restart of node {}/{} succeeded - continuing with next node", node.getIp(), node.getHostname());

                            nodeFinished(CassandraFrameworkProtos.NodeJobStatus.newBuilder(nodeJobStatus)
                                .setRunning(false)
                                .setFinishedTimestamp(System.currentTimeMillis())
                                .build(), currentJob);
                        } else if (nodeJobStatus.getStartedTimestamp() + RESTART_NODE_TIMEOUT_MILLIS < System.currentTimeMillis()) {
                            LOGGER.warn("Restart of node {}/{} did not succeed after " + RESTART_NODE_TIMEOUT_MILLIS + "ms - continuing with next node", node.getIp(), node.getHostname());

                            nodeFinished(CassandraFrameworkProtos.NodeJobStatus.newBuilder(nodeJobStatus)
                                .setRunning(false)
                                .setFailed(true)
                                .setFailureMessage("Timeout after " + RESTART_NODE_TIMEOUT_MILLIS + "ms")
                                .setFinishedTimestamp(System.currentTimeMillis())
                                .build(), currentJob);
                        }

                        return;
                    case RESTART:
                        // node is still restarting
                        return;
                    case STOP:
                    case TERMINATE:
                        // OK, something happend to the targetRunState
                        return;
                }
            }
        }

        if (currentJob.getAborted() && !currentJob.hasCurrentNode()) {
            jobsState.setCurrentJob(null);
            // TODO record aborted job in history??
            return;
        }

        if (!currentJob.hasCurrentNode()) {
            List<String> remainingNodes = new ArrayList<>(currentJob.getRemainingNodesList());
            if (remainingNodes.isEmpty()) {
                jobsState.finishJob(currentJob);
                return;
            }

            if (!remainingNodes.remove(executorId)) {
                return;
            }

            if (!nodeForExecutorId.isPresent()) {
                currentJob = CassandraFrameworkProtos.ClusterJobStatus.newBuilder()
                    .clearRemainingNodes()
                    .addAllRemainingNodes(remainingNodes)
                    .build();
                jobsState.setCurrentJob(currentJob);
                return;
            }

            CassandraFrameworkProtos.CassandraNode node = nodeForExecutorId.get();

            CassandraFrameworkProtos.NodeJobStatus currentNode = CassandraFrameworkProtos.NodeJobStatus.newBuilder()
                .setExecutorId(node.getCassandraNodeExecutor().getExecutorId())
                .setTaskId(executorId + '.' + currentJob.getJobType().name())
                .setJobType(currentJob.getJobType())
                .setStartedTimestamp(System.currentTimeMillis())
                .build();

            switch (node.getTargetRunState()) {
                case RUN:
                    cluster.updateNodeTargetRunState(node, CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART);
                    // fall through
                case RESTART:
                    jobsState.updateJobCurrentNode(currentJob, currentNode);

                    LOGGER.info("Restarting node {}/{} as part of cluster-restart", node.getIp(), node.getHostname());

                    break;
                case STOP:
                case TERMINATE:
                    // nothing to do for that targetRunState

                    nodeFinished(CassandraFrameworkProtos.NodeJobStatus.newBuilder(currentNode)
                        .setRunning(false)
                        .setFailed(true)
                        .setFailureMessage("Node cannot be restarted when in status " + node.getTargetRunState().name())
                        .setFinishedTimestamp(System.currentTimeMillis())
                        .build(), currentJob);

                    break;
            }
        }
    }

    @Override
    public void onNodeJobStatus(CassandraFrameworkProtos.ClusterJobStatus currentJob, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) {

    }
}
