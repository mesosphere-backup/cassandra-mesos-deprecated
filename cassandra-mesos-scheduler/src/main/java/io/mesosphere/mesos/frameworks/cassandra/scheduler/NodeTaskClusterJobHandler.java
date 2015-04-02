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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NodeTaskClusterJobHandler extends ClusterJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTaskClusterJobHandler.class);

    public NodeTaskClusterJobHandler(CassandraCluster cluster, PersistedCassandraClusterJobs jobsState) {
        super(cluster, jobsState);
    }

    @Override
    public void handleTaskOffer(CassandraFrameworkProtos.ClusterJobStatus currentJob, String executorId, Optional<CassandraFrameworkProtos.CassandraNode> nodeForExecutorId, TasksForOffer tasksForOffer) {
        if (currentJob.hasCurrentNode()) {
            CassandraFrameworkProtos.NodeJobStatus nodJobStatus = currentJob.getCurrentNode();
            if (executorId.equals(nodJobStatus.getExecutorId())) {
                // submit status request
                tasksForOffer.getSubmitTasks().add(CassandraFrameworkProtos.TaskDetails.newBuilder()
                    .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                    .build());

                LOGGER.info("Inquiring cluster job status for {} from {}", currentJob.getJobType().name(),
                    nodJobStatus.getExecutorId());

                return;
            }
            return;
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
                rejectNode(remainingNodes);
                return;
            }

            CassandraFrameworkProtos.CassandraNode node = nodeForExecutorId.get();

            if (node.getTargetRunState() != CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN
                || !cluster.isLiveNode(node)) {
                rejectNode(remainingNodes);
                return;
            }

            final CassandraFrameworkProtos.TaskDetails taskDetails = CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB)
                .setNodeJobTask(CassandraFrameworkProtos.NodeJobTask.newBuilder().setJobType(currentJob.getJobType()))
                .build();
            CassandraFrameworkProtos.CassandraNodeTask cassandraNodeTask = CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.CLUSTER_JOB)
                .setTaskId(executorId + '.' + currentJob.getJobType().name())
                .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                    .setCpuCores(0.1)
                    .setMemMb(16)
                    .setDiskMb(16))
                .setTaskDetails(taskDetails)
                .build();
            tasksForOffer.getLaunchTasks().add(cassandraNodeTask);

            CassandraFrameworkProtos.NodeJobStatus currentNode = CassandraFrameworkProtos.NodeJobStatus.newBuilder()
                .setExecutorId(node.getCassandraNodeExecutor().getExecutorId())
                .setTaskId(cassandraNodeTask.getTaskId())
                .setJobType(currentJob.getJobType())
                .setStartedTimestamp(System.currentTimeMillis())
                .build();
            jobsState.updateJobCurrentNode(currentJob, currentNode);

            LOGGER.info("Starting cluster job {} on {}/{}", currentJob.getJobType().name(), node.getIp(),
                node.getHostname());
        }
    }

    private void rejectNode(List<String> remainingNodes) {
        CassandraFrameworkProtos.ClusterJobStatus currentJob;
        currentJob = CassandraFrameworkProtos.ClusterJobStatus.newBuilder()
            .clearRemainingNodes()
            .addAllRemainingNodes(remainingNodes)
            .build();
        jobsState.setCurrentJob(currentJob);
    }

    @Override
    public void onNodeJobStatus(CassandraFrameworkProtos.ClusterJobStatus currentJob, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) {
        LOGGER.info("Got node job status from {}, running={}", nodeJobStatus.getExecutorId(), nodeJobStatus.getRunning());

        if (currentJob.getCurrentNode() != null && currentJob.getCurrentNode().getExecutorId().equals(nodeJobStatus.getExecutorId())) {
            if (nodeJobStatus.getRunning()) {
                jobsState.setCurrentJob(CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob)
                    .setCurrentNode(nodeJobStatus)
                    .build());
            } else {
                nodeFinished(nodeJobStatus, currentJob);
            }
        }
    }
}
