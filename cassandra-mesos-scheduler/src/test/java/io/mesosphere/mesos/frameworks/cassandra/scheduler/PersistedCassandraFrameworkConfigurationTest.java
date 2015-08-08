package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.common.collect.Lists;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.state.InMemoryState;
import org.junit.Test;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraConfigRole;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import static junit.framework.Assert.assertEquals;

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
                "",
                false,
                true,
                "RACK5",
                "DC5",
                Lists.<CassandraFrameworkProtos.ExternalDc>newArrayList()
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
}
