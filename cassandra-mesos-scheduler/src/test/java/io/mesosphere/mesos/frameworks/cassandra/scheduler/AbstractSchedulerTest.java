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
import io.mesosphere.mesos.util.SystemClock;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.assertj.core.api.Condition;

import java.util.UUID;

public abstract class AbstractSchedulerTest {
    protected final Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(randomID()).build();

    protected State state;

    protected PersistedCassandraClusterState clusterState;
    protected PersistedCassandraFrameworkConfiguration configuration;

    protected CassandraCluster cluster;

    protected int activeNodes;
    @SuppressWarnings("unchecked")
    protected final Tuple2<Protos.SlaveID, String>[] slaves = new Tuple2[]{
            Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.1.1.1"),
            Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.2.2.2"),
            Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.3.3.3"),
            Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.4.4.4")
    };

    protected void cleanState() {
        // start with clean state
        state = new InMemoryState();

        configuration = new PersistedCassandraFrameworkConfiguration(
                state,
                "test-cluster",
                0, // health-check
                0, // bootstrap-grace-time
                "2.1.4",
            2, 4096, 4096, 0,
            3, 2,
            "*",
            ".",
            true,
            false);

        cluster = new CassandraCluster(new SystemClock(),
                "http://127.0.0.1:65535",
                new ExecutorCounter(state, 0L),
                new PersistedCassandraClusterState(state, 3, 2),
                new PersistedCassandraClusterHealthCheckHistory(state),
                new PersistedCassandraClusterJobs(state),
                configuration);

        clusterState = cluster.getClusterState();
    }

    protected Protos.Offer createOffer(Tuple2<Protos.SlaveID, String> slave) {
        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
                .setFrameworkId(frameworkId)
                .setHostname(slave._2)
                .setId(Protos.OfferID.newBuilder().setValue(randomID()))
                .setSlaveId(slave._1);

        builder.addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setRole("*")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                        .setName("ports")
                        .setRole("*")
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
                        .setVersion("2.1.4")
                        .setNativeTransportRunning(true)
                        .setRpcServerRunning(true)
                )
                .build();
    }

    protected static String randomID() {
        return UUID.randomUUID().toString();
    }

    static Condition<? super CassandraFrameworkProtos.HealthCheckDetails> operationMode(final String operationMode) {
        return new Condition<CassandraFrameworkProtos.HealthCheckDetails>() {
            @Override
            public boolean matches(CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
                return healthCheckDetails != null && healthCheckDetails.getInfo().getOperationMode().equals(operationMode);
            }
        };
    }

    static Condition<? super CassandraFrameworkProtos.HealthCheckDetails> healthy() {
        return new Condition<CassandraFrameworkProtos.HealthCheckDetails>() {
            @Override
            public boolean matches(CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
                return healthCheckDetails.getHealthy();
            }
        };
    }
}
