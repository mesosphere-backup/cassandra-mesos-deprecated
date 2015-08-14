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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.ClusterJobType;

public class NodeTaskClusterJobHandler extends ClusterJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTaskClusterJobHandler.class);

    public NodeTaskClusterJobHandler(@NotNull final CassandraCluster cluster, @NotNull final PersistedCassandraClusterJobs jobsState) {
        super(cluster, jobsState);
    }

    @Override
    public void handleTaskOffer(@NotNull final CassandraFrameworkProtos.ClusterJobStatus currentJob, @NotNull final String executorId, @NotNull final Optional<CassandraFrameworkProtos.CassandraNode> nodeForExecutorId, @NotNull final TasksForOffer tasksForOffer) {
        if (currentJob.hasCurrentNode()) {
            final CassandraFrameworkProtos.NodeJobStatus nodJobStatus = currentJob.getCurrentNode();
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
            final List<String> remainingNodes = new ArrayList<>(currentJob.getRemainingNodesList());
            if (remainingNodes.isEmpty()) {
                jobsState.finishJob(currentJob);
                return;
            }

            if (!remainingNodes.remove(executorId)) {
                return;
            }

            if (!nodeForExecutorId.isPresent()) {
                rejectNode(currentJob, remainingNodes);
                return;
            }

            final CassandraFrameworkProtos.CassandraNode node = nodeForExecutorId.get();

            if (node.getTargetRunState() != CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN
                || !cluster.isLiveNode(node)) {
                rejectNode(currentJob, remainingNodes);
                return;
            }

            final CassandraFrameworkProtos.NodeJobTask.Builder nodeJobTaskBuilder = CassandraFrameworkProtos.NodeJobTask.newBuilder().setJobType(currentJob.getJobType());
            if (Arrays.asList(ClusterJobType.BACKUP, ClusterJobType.RESTORE).contains(currentJob.getJobType())) {
                nodeJobTaskBuilder.setFirstNodeInJob(currentJob.getCompletedNodesCount() == 0);

                final PersistedCassandraFrameworkConfiguration configuration = cluster.getConfiguration();
                final String backupDir = configuration.getDefaultConfigRole().getBackupDirectory()
                        + "/" + configuration.get().getFrameworkName()
                        + "/" + node.getCassandraNodeExecutor().getExecutorId()
                        + "/" + currentJob.getBackupName();

                nodeJobTaskBuilder.setBackupDir(backupDir);
            }

            final CassandraFrameworkProtos.TaskDetails taskDetails = CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB)
                .setNodeJobTask(nodeJobTaskBuilder)
                .build();
            final CassandraFrameworkProtos.CassandraNodeTask cassandraNodeTask = CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.CLUSTER_JOB)
                .setTaskId(executorId + '.' + currentJob.getJobType().name())
                .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                    .setCpuCores(0.1)
                    .setMemMb(16)
                    .setDiskMb(16))
                .setTaskDetails(taskDetails)
                .build();
            tasksForOffer.getLaunchTasks().add(cassandraNodeTask);

            final CassandraFrameworkProtos.NodeJobStatus currentNode = CassandraFrameworkProtos.NodeJobStatus.newBuilder()
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

    private void rejectNode(@NotNull final CassandraFrameworkProtos.ClusterJobStatus currentJob, @NotNull final List<String> remainingNodes) {
        final CassandraFrameworkProtos.ClusterJobStatus updatedJob =
                CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob)
                        .clearRemainingNodes()
                        .addAllRemainingNodes(remainingNodes)
                        .build();
        jobsState.setCurrentJob(updatedJob);
    }

    @Override
    public void onNodeJobStatus(@NotNull final CassandraFrameworkProtos.ClusterJobStatus currentJob, @NotNull final CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) {
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
