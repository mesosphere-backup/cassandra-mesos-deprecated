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

import io.mesosphere.mesos.util.SystemClock;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;

import java.util.UUID;

public abstract class AbstractSchedulerTest {
    final Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(randomID()).build();

    State state;
    PersistedCassandraFrameworkConfiguration configuration;
    PersistedCassandraClusterHealthCheckHistory healthCheckHistory;
    PersistedCassandraClusterState clusterState;
    CassandraCluster cluster;

    final Tuple2<Protos.SlaveID, String> slave1 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.1.1.1");
    final Tuple2<Protos.SlaveID, String> slave2 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.2.2.2");
    final Tuple2<Protos.SlaveID, String> slave3 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.3.3.3");

    protected void cleanState() {
        // start with clean state
        state = new InMemoryState();

        configuration = new PersistedCassandraFrameworkConfiguration(
                state,
                "test-cluster",
                "2.1.2",
                3, // node count
                2, // seed count
                2, // CPUs
                4096, // memMb
                4096, // diskMb
                10, // health-check
                10 // bootstrap-grace-time
        );

        healthCheckHistory = new PersistedCassandraClusterHealthCheckHistory(state);
        clusterState = new PersistedCassandraClusterState(state);

        cluster = new CassandraCluster(new SystemClock(),
                "http://127.0.0.1:65535",
                new ExecutorCounter(state, 0L),
                clusterState,
                healthCheckHistory,
                configuration);
    }

    protected Protos.Offer createOffer(Tuple2<Protos.SlaveID, String> slave) {
        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
                .setFrameworkId(frameworkId)
                .setHostname(slave._2)
                .setId(Protos.OfferID.newBuilder().setValue(randomID()))
                .setSlaveId(slave._1);

        builder.addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                        .setName("ports")
                        .setType(Protos.Value.Type.RANGES)
                        .setRanges(Protos.Value.Ranges.newBuilder()
                                .addRange(Protos.Value.Range.newBuilder().setBegin(7000).setEnd(10000)))
        );

        return builder.build();
    }

    protected static CassandraFrameworkProtos.HealthCheckDetails healthCheckDetailsFailed() {
        return CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
                .setHealthy(false)
                .setMsg("foo bar")
                .build();
    }

    protected static CassandraFrameworkProtos.HealthCheckDetails healthCheckDetailsSuccess(String operationMode, boolean joined) {
        return CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
                .setHealthy(true)
                .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                                .setClusterName("cluster-name")
                                .setDataCenter("dc")
                                .setRack("rac")
                                .setJoined(joined)
                                .setOperationMode(operationMode)
                                .setUptimeMillis(1234)
                                .setVersion("2.1.2")
                )
                .build();
    }

    protected static String randomID() {
        return UUID.randomUUID().toString();
    }
}
