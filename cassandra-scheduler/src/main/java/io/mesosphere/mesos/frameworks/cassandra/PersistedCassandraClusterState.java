package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.CassandraFrameworkProtosUtils.cassandraNodeHostnameEq;
import static io.mesosphere.mesos.util.Functions.append;

final class PersistedCassandraClusterState extends StatePersistedObject<CassandraFrameworkProtos.CassandraClusterState> {
    public PersistedCassandraClusterState(@NotNull final State state) {
        super(
            "CassandraClusterState",
            state,
            new Supplier<CassandraFrameworkProtos.CassandraClusterState>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterState get() {
                    return CassandraFrameworkProtos.CassandraClusterState.newBuilder().build();
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
        final FluentIterable<CassandraFrameworkProtos.CassandraNode> filterNot = from(nodes())
            .filter(not(cassandraNodeHostnameEq(node.getHostname())));
        nodes(append(newArrayList(filterNot), node));
    }
}
