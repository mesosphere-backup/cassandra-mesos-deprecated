package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.state.InMemoryState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.ExternalDc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SeedManagerTest {
    private SeedManager seedManager;

    private String jsonResponse = seedsJson(Collections.<String>emptyList());

    @Before
    public void before() {
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
                "RACK1",
                "DC1",
                Arrays.asList(ExternalDc.newBuilder()
                        .setName("dc")
                        .setUrl("http://dc")
                        .build()),
                "name",
                false
        );

        seedManager = new SeedManager(config, new ObjectMapper(), new SystemClock()) {
            @Override
            @Nullable
            protected JsonNode fetchJson(@NotNull final String url) {
                try {
                    return new ObjectMapper().readTree(jsonResponse);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }

    @Test
    public void getSeeds_syncSeeds() {
        assertTrue(seedManager.getSeeds().isEmpty());

        jsonResponse = seedsJson(Arrays.asList("1", "2"));
        seedManager.syncSeeds();
        assertEquals(Arrays.asList("1", "2"), seedManager.getSeeds());
    }

    private static String seedsJson(List<String> seeds) {
        return "{\"seeds\": [" + (seeds.isEmpty() ? "" : "\"" + Joiner.on("\", \"").join(seeds) + "\"") + "]}";
    }
}
