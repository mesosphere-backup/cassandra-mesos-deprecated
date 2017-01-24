package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.ExternalDc;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.junit.Test;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraConfigRole;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import static io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Futures.await;
import static junit.framework.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class PersistedCassandraFrameworkConfigurationTest {
    @Test
    public void testGetDefaultRackDc() {
        InMemoryState state = new InMemoryState();

        PersistedCassandraFrameworkConfiguration config = new PersistedCassandraFrameworkConfiguration(
                state,
                "name",
                60,
                30,
                "2.1",
                0.5,
                1024,
                1024,
                512,
                1,
                1,
                "role",
                "./backup",
                ".",
                false,
                true,
                "RACK5",
                "DC5",
                Lists.<ExternalDc>newArrayList(),
                "clusterName",
                false
        );

        CassandraFrameworkProtos.RackDc rackDc = config.getDefaultRackDc();
        assertEquals("RACK5", rackDc.getRack());
        assertEquals("DC5", rackDc.getDc());

        // backward compatibility: if rackDc is not defined - use defaults
        CassandraFrameworkConfiguration.Builder builder = CassandraFrameworkConfiguration.newBuilder(config.get());
        builder.setDefaultConfigRole(CassandraConfigRole.newBuilder(builder.getDefaultConfigRole()).clearRackDc());
        config.setValue(builder.build());

        rackDc = config.getDefaultRackDc();
        assertEquals("RAC1", rackDc.getRack());
        assertEquals("DC1", rackDc.getDc());
    }

    @Test
    public void separationOfFrameworkNameAndClusterNamePreservesOriginalClusterName() throws Exception {
        final State state = createInitializedState("CassandraFrameworkConfiguration", "/CassandraFrameworkConfiguration_v0.2.0.bin");

        final PersistedCassandraFrameworkConfiguration configuration = new PersistedCassandraFrameworkConfiguration(
            state,
            "frameworkName should be cassandra.cluster not this value",
            15,
            15,
            "2.1.4",
            1.0,
            2048,
            2048,
            1024,
            1,
            1,
            "*",
            "./backup",
            ".",
            true,
            true,
            "rack1",
            "dc1",
            Collections.<ExternalDc>emptyList(),
            "clusterName should be cassandra.cluster not this value",
            false
        );

        assertThat(configuration.frameworkName()).isEqualTo("cassandra.cluster");
        assertThat(configuration.clusterName()).isEqualTo("cassandra.cluster");
    }

    @Test
    public void generatingNewConfigAllowsFrameworkNameAndClusterNameToBeDifferent() throws Exception {
        final State state = new InMemoryState();

        final PersistedCassandraFrameworkConfiguration configuration = new PersistedCassandraFrameworkConfiguration(
            state,
            "cassandra.frameworkName",
            15,
            15,
            "2.1.4",
            1.0,
            2048,
            2048,
            1024,
            1,
            1,
            "*",
            "./backup",
            ".",
            true,
            true,
            "rack1",
            "dc1",
            Collections.<ExternalDc>emptyList(),
            "clusterName",
            false
        );

        assertThat(configuration.frameworkName()).isEqualTo("cassandra.frameworkName");
        assertThat(configuration.clusterName()).isEqualTo("clusterName");
    }

    @NotNull
    private State createInitializedState(@NotNull final String varName, @NotNull final String resourceName) throws IOException {
        final State state = new InMemoryState();
        final Variable var = await(state.fetch(varName));
        final Variable mut = var.mutate(readConfigurationFile(resourceName));
        await(state.store(mut));
        return state;
    }

    @NotNull
    private byte[] readConfigurationFile(@NotNull final String resourceName) throws IOException {
        final InputStream resourceAsStream = this.getClass().getResourceAsStream(resourceName);
        return ByteStreams.toByteArray(resourceAsStream);
    }
}
