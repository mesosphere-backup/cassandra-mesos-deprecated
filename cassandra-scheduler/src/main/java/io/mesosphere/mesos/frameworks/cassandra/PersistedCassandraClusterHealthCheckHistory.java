package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;

import java.util.List;

final class PersistedCassandraClusterHealthCheckHistory extends StatePersistedObject<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory> {
    public PersistedCassandraClusterHealthCheckHistory(
        @NotNull final State state
    ) {
        super(
            "CassandraClusterHealthCheckHistory",
            state,
            new Supplier<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterHealthCheckHistory get() {
                    return CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.newBuilder().build();
                }
            },
            new Function<byte[], CassandraFrameworkProtos.CassandraClusterHealthCheckHistory>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterHealthCheckHistory apply(final byte[] input) {
                    try {
                        return CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.parseFrom(input);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ProtoUtils.RuntimeInvalidProtocolBufferException(e);
                    }
                }
            },
            new Function<CassandraFrameworkProtos.CassandraClusterHealthCheckHistory, byte[]>() {
                @Override
                public byte[] apply(final CassandraFrameworkProtos.CassandraClusterHealthCheckHistory input) {
                    return input.toByteArray();
                }
            }
        );
    }

    @NotNull
    public List<CassandraFrameworkProtos.HealthCheckHistoryEntry> entries() {
        return get().getEntriesList();
    }

    public void record(@NotNull final CassandraFrameworkProtos.HealthCheckHistoryEntry entry) {
        //TODO(BenWhitehead): Bound this
        setValue(
            CassandraFrameworkProtos.CassandraClusterHealthCheckHistory.newBuilder(get())
                .addEntries(entry)
                .build()
        );
    }

    public CassandraFrameworkProtos.HealthCheckHistoryEntry last(String executorId) {
        List<CassandraFrameworkProtos.HealthCheckHistoryEntry> list = get().getEntriesList();
        return list != null && !list.isEmpty() ?
                list.get(list.size()-1) :
                null;
    }
}
