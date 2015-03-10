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
        if (list == null)
            return null;
        for (int i = list.size() - 1; i >= 0; i--) {
            CassandraFrameworkProtos.HealthCheckHistoryEntry hc = list.get(i);
            if (executorId.equals(hc.getExecutorId()))
                return hc;
        }
        return null;
    }
}
