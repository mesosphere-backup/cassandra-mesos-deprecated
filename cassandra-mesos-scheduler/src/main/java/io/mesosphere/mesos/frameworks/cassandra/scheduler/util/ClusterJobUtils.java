package io.mesosphere.mesos.frameworks.cassandra.scheduler.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.ClusterJobType;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.api.StreamingJsonResponse;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.core.Response;
import java.io.IOException;

public final class ClusterJobUtils {

    private ClusterJobUtils() {}

    public static Response startJob(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory, @NotNull ClusterJobType type) {
        final boolean started = cluster.startClusterTask(type);
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("started", started);
            }
        });
    }

    public static Response abortJob(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory, @NotNull ClusterJobType type) {
        final boolean aborted = cluster.abortClusterJob(type);
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("aborted", aborted);
            }
        });
    }

    public static Response jobStatus(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory, @NotNull ClusterJobType type, @NotNull final String name) {
        final CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getCurrentClusterJob(type);
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("running", repairJob != null);
                JaxRsUtils.writeClusterJob(cluster, json, name, repairJob);
            }
        });
    }

    public static Response lastJob(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory, @NotNull ClusterJobType type, @NotNull final String name) {
        final CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getLastClusterJob(type);
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("present", repairJob != null);
                JaxRsUtils.writeClusterJob(cluster, json, name, repairJob);
            }
        });
    }
}
