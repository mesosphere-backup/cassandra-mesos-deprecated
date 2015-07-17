package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import io.mesosphere.mesos.util.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.ExternalDc;

public class SeedManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeedManager.class);

    @NotNull
    private final PersistedCassandraFrameworkConfiguration configuration;

    @NotNull
    private final ObjectMapper objectMapper;

    @NotNull
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> syncingTask;

    @NotNull
    private final Clock clock;

    public SeedManager(@NotNull final PersistedCassandraFrameworkConfiguration configuration,
                       @NotNull final ObjectMapper objectMapper,
                       @NotNull final Clock clock) {
        this.configuration = configuration;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @NotNull
    public List<String> getSeeds() {
        final List<String> seeds = newArrayList();

        for (final ExternalDc dc : configuration.get().getExternalDcsList()) {
            seeds.addAll(dc.getSeedsList());
        }

        return seeds;
    }

    public void startSyncingSeeds(final long periodSeconds) {
        stopSyncingSeeds();
        LOGGER.debug("Scheduling background syncing task to run every {} seconds", periodSeconds);
        syncingTask = scheduledExecutorService.scheduleAtFixedRate(
                new SyncingTask(),
                0,
                periodSeconds,
                TimeUnit.SECONDS
        );
    }

    private void stopSyncingSeeds() {
        if (syncingTask != null) {
            LOGGER.debug("Stopping scheduled background syncing task");
            syncingTask.cancel(true);
            syncingTask = null;
        }
    }

    public void syncSeeds() {
        LOGGER.info("Syncing seeds ...");
        for (ExternalDc dc : configuration.get().getExternalDcsList()) {
            fetchSeeds(dc);
        }
    }

    private void fetchSeeds(@NotNull final ExternalDc dc) {
        String url = dc.getUrl();
        if (url.endsWith("/")) url = url.substring(0, url.length() - 1);
        url += "/node/seed/all";

        JsonNode node = fetchJson(url);
        if (node == null) return;

        JsonNode seedsArr = node.get("seeds");
        List<String> seeds = newArrayList();
        for (JsonNode ip : seedsArr) {
            seeds.add(ip.asText());
        }

        LOGGER.info("Fetched seeds for dc \"" + dc.getName() + "\": " + Joiner.on(", ").join(seeds));
        updateDcSeeds(dc, seeds);
    }

    @Nullable
    protected JsonNode fetchJson(@NotNull final String url) {
        HttpURLConnection c = null;
        try {
            c = (HttpURLConnection)new URL(url).openConnection();
            return objectMapper.readTree(c.getInputStream());
        } catch (IOException e) {
            LOGGER.warn("", e);
        } finally {
            if (c != null) c.disconnect();
        }

        return null;
    }

    private int externalDcIdx(@NotNull final String name) {
        final List<ExternalDc> dcs = configuration.get().getExternalDcsList();

        for (int i = 0; i < dcs.size(); i++) {
            ExternalDc dc = dcs.get(i);
            if (dc.getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private void updateDcSeeds(@NotNull final ExternalDc dc, @NotNull final List<String> seeds) {
        int idx = externalDcIdx(dc.getName());

        ExternalDc newDc = ExternalDc.newBuilder(dc)
                .clearSeeds().addAllSeeds(seeds)
                .setSeedFetchTime(clock.now().getMillis()).build();

        CassandraFrameworkConfiguration config = CassandraFrameworkConfiguration.newBuilder(configuration.get())
                .setExternalDcs(idx, newDc)
                .build();

        configuration.setValue(config);
    }

    private class SyncingTask implements Runnable {
        @Override
        public void run() {
            syncSeeds();
        }
    }
}
