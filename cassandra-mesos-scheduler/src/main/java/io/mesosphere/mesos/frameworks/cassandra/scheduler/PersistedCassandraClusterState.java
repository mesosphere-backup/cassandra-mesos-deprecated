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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static io.mesosphere.mesos.util.Functions.append;

public final class PersistedCassandraClusterState extends StatePersistedObject<CassandraFrameworkProtos.CassandraClusterState> {
    public PersistedCassandraClusterState(
            @NotNull final State state,
            final int executorCount,
            final int seedCount) {
        super(
            "CassandraClusterState",
            state,
            new Supplier<CassandraFrameworkProtos.CassandraClusterState>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterState get() {
                    return CassandraFrameworkProtos.CassandraClusterState.newBuilder()
                        .setNodesToAcquire(executorCount)
                        .setSeedsToAcquire(seedCount)
                        .build();
                }
            },
            new Function<byte[], CassandraFrameworkProtos.CassandraClusterState>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterState apply(final byte[] input) {
                    try {
                        return CassandraFrameworkProtos.CassandraClusterState.parseFrom(input);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ProtoUtils.RuntimeInvalidProtocolBufferException(e);
                    }
                }
            },
            new Function<CassandraFrameworkProtos.CassandraClusterState, byte[]>() {
                @Override
                public byte[] apply(final CassandraFrameworkProtos.CassandraClusterState input) {
                    return input.toByteArray();
                }
            }
        );
    }

    @NotNull
    public List<CassandraFrameworkProtos.CassandraNode> nodes() {
        return get().getNodesList();
    }

    public void nodes(@NotNull final List<CassandraFrameworkProtos.CassandraNode> nodes) {
        setValue(
            CassandraFrameworkProtos.CassandraClusterState.newBuilder(get())
                .clearNodes()
                .addAllNodes(nodes)
                .build()
        );
    }

    @NotNull
    public List<CassandraFrameworkProtos.ExecutorMetadata> executorMetadata() {
        return get().getExecutorMetadataList();
    }

    public void executorMetadata(@NotNull final List<CassandraFrameworkProtos.ExecutorMetadata> executorMetadata) {
        setValue(
            CassandraFrameworkProtos.CassandraClusterState.newBuilder(get())
                .clearExecutorMetadata()
                .addAllExecutorMetadata(executorMetadata)
                .build()
        );
    }

    /**
     * Add a node, making sure to replace any previoud node with the same hostname
     */
    public void addOrSetNode(final CassandraFrameworkProtos.CassandraNode node) {
        List<CassandraFrameworkProtos.CassandraNode> nodeList = new ArrayList<>(nodes());
        for (int i = 0; i < nodeList.size(); i++) {
            CassandraFrameworkProtos.CassandraNode candidate = nodeList.get(i);
            if (node.getHostname().equals(candidate.getHostname())) {
                nodeList.set(i, node);
                nodes(nodeList);
                return;
            }
        }
        nodeList.add(node);
        nodes(nodeList);
    }

    /**
     * Sets the {@code needsConfigUpdate} flag on all nodes and update the given {@code node}.
     */
    public void setNodeAndUpdateConfig(CassandraFrameworkProtos.CassandraNode.Builder node) {
        List<CassandraFrameworkProtos.CassandraNode> nodeList = new ArrayList<>();
        for (CassandraFrameworkProtos.CassandraNode candidate : nodes()) {
            if (node.getHostname().equals(candidate.getHostname())) {
                nodeList.add(node
                    .setNeedsConfigUpdate(true)
                    .build());
            } else {
                candidate = CassandraFrameworkProtos.CassandraNode.newBuilder(candidate)
                    .setNeedsConfigUpdate(true)
                    .build();
                nodeList.add(candidate);
            }
        }
        nodes(nodeList);
    }

    public NodeCounts nodeCounts() {
        int nodeCount = 0;
        int seedCount = 0;
        for (CassandraFrameworkProtos.CassandraNode n : nodes()) {
            if (n.getTargetRunState() == CassandraFrameworkProtos.CassandraNode.TargetRunState.TERMINATE) {
                // not a live node - do not count
                continue;
            }

            nodeCount++;
            if (n.getSeed())
                seedCount++;
        }
        return new NodeCounts(nodeCount, seedCount);
    }

    public void updateLastServerLaunchTimestamp(long lastServerLaunchTimestamp) {
        setValue(
                CassandraFrameworkProtos.CassandraClusterState.newBuilder(get())
                        .setLastServerLaunchTimestamp(lastServerLaunchTimestamp)
                        .build()
        );
    }

    public void replaceNode(String ip) {
        CassandraFrameworkProtos.CassandraClusterState.Builder builder = CassandraFrameworkProtos.CassandraClusterState.newBuilder(get());
        setValue(
            builder
                .addReplaceNodeIps(ip)
                .setNodesToAcquire(builder.getNodesToAcquire() + 1)
                .build()
        );
    }

    public void nodeAcquired(CassandraFrameworkProtos.CassandraNode newNode) {
        CassandraFrameworkProtos.CassandraClusterState.Builder builder = CassandraFrameworkProtos.CassandraClusterState.newBuilder(get());

        if (newNode.hasReplacementForIp()) {
            List<String> replacements = new ArrayList<>(builder.getReplaceNodeIpsList());
            replacements.remove(newNode.getReplacementForIp());
            builder.clearReplaceNodeIps().addAllReplaceNodeIps(replacements);
        }

        setValue(
            builder
                .clearNodes()
                .addAllNodes(append(
                    nodes(),
                    newNode
                ))
                .setNodesToAcquire(builder.getNodesToAcquire() - 1)
                .build()
        );
    }

    public String nextReplacementIp() {
        List<String> list = get().getReplaceNodeIpsList();
        return list.isEmpty() ? null : list.get(0);
    }

    public void acquireNewNodes(int newNodeCount) {
        CassandraFrameworkProtos.CassandraClusterState.Builder builder = CassandraFrameworkProtos.CassandraClusterState.newBuilder(get());
        setValue(
            builder
                .setNodesToAcquire(builder.getNodesToAcquire() + newNodeCount)
                .build()
        );
    }

    public void nodeReplaced(CassandraFrameworkProtos.CassandraNode cassandraNode) {
        CassandraFrameworkProtos.CassandraClusterState prev = get();
        CassandraFrameworkProtos.CassandraClusterState.Builder builder = CassandraFrameworkProtos.CassandraClusterState.newBuilder(prev)
            .clearNodes();

        for (CassandraFrameworkProtos.CassandraNode node : prev.getNodesList()) {
            // add all but the node that has been replaced (effectively removing it)
            if (node.getIp().equals(cassandraNode.getIp())) {
                builder.addNodes(CassandraFrameworkProtos.CassandraNode.newBuilder(cassandraNode).clearReplacementForIp());
            } else if (!node.getIp().equals(cassandraNode.getReplacementForIp())) {
                builder.addNodes(node);
            }
        }

        setValue(builder.build());
    }
    public boolean doAcquireNewNodeAsSeed() {
        CassandraFrameworkProtos.CassandraClusterState value = get();
        if (value.getSeedsToAcquire() == 0) {
            return false;
        }
        setValue(CassandraFrameworkProtos.CassandraClusterState.newBuilder(value)
            .setSeedsToAcquire(value.getSeedsToAcquire() - 1)
            .build());
        return true;
    }
}
