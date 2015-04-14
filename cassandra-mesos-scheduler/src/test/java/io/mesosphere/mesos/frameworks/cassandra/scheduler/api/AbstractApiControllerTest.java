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
package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.collect.Sets;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.AbstractCassandraSchedulerTest;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.InetAddressUtils;
import io.mesosphere.mesos.util.Tuple2;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

public abstract class AbstractApiControllerTest extends AbstractCassandraSchedulerTest {
    @NotNull
    private final ObjectMapper mapper = new ObjectMapper();
    @NotNull
    private final JsonFactory factory = new JsonFactory();
    @Nullable
    protected URI httpServerBaseUri;
    @Nullable
    protected HttpServer httpServer;

    protected void addNode(@NotNull final String executorId, @NotNull final String ip) {
        cluster.getClusterState().addOrSetNode(CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setCassandraNodeExecutor(
                CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder(CassandraFrameworkProtos.CassandraNodeExecutor.getDefaultInstance())
                    .setExecutorId(executorId)
                    .setSource("source")
                    .addCommand("comand")
                    .setResources(someResources())
            )
            .setHostname(ip)
            .setIp(ip)
            .setSeed(false)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder()
                .setIp(ip)
                .setJmxPort(7199))
            .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                .setTaskId(executorId + ".server")
                .setType(CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER)
                .setResources(someResources()))
            .build());
        cluster.recordHealthCheck(executorId, healthCheckDetailsSuccess("NORMAL", true));
    }

    @NotNull
    protected Tuple2<Integer, JsonNode> getJson(@NotNull final String rel) throws Exception {
        return fetchJson(rel, false);
    }

    @NotNull
    protected Tuple2<Integer, JsonNode> postJson(@NotNull final String rel) throws Exception {
        return fetchJson(rel, true);
    }

    @NotNull
    protected Tuple2<Integer, JsonNode> fetchJson(final String rel, final boolean post) throws Exception {
        final JsonFactory factory = new JsonFactory();
        final HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
            conn.setRequestMethod(post ? "POST" : "GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.connect();

            final int responseCode = conn.getResponseCode();

            InputStream in;
            try {
                in = conn.getInputStream();
            } catch (final IOException e) {
                in = conn.getErrorStream();
            }
            if (in == null) {
                return Tuple2.tuple2(responseCode, (JsonNode) MissingNode.getInstance());
            }

            assertEquals("application/json", conn.getHeaderField("Content-Type"));

            try {
                final ObjectMapper om = new ObjectMapper();
                return Tuple2.tuple2(responseCode, om.reader()
                    .with(factory)
                    .readTree(in));
            } finally {
                in.close();
            }
        } finally {
            conn.disconnect();
        }
    }

    @NotNull
    protected Tuple2<Integer, String> fetchText(final String rel, final String expectedContentType) throws Exception {
        final HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
            conn.setRequestProperty("Accept", expectedContentType);
            conn.connect();

            final int responseCode = conn.getResponseCode();

            InputStream in;
            try {
                in = conn.getInputStream();
            } catch (final IOException e) {
                in = conn.getErrorStream();
            }
            if (in == null) {
                return Tuple2.tuple2(responseCode, "");
            }

            assertEquals(expectedContentType, conn.getHeaderField("Content-Type"));

            try {
                final StringBuilder sb = new StringBuilder();
                int rd;
                while ((rd = in.read()) != -1) {
                    sb.append((char) rd);
                }
                return Tuple2.tuple2(responseCode, sb.toString());
            } finally {
                in.close();
            }
        } finally {
            conn.disconnect();
        }
    }

    @NotNull
    protected URI resolve(final String rel) {
        return checkNotNull(httpServerBaseUri).resolve(rel);
    }

    @Before
    public void cleanState() {
        super.cleanState();

        try {
            try (ServerSocket sock = new ServerSocket(0)) {
                httpServerBaseUri = URI.create(String.format("http://%s:%d/", InetAddressUtils.formatInetAddress(InetAddress.getLoopbackAddress()), sock.getLocalPort()));
            }

            final ResourceConfig rc = new ResourceConfig()
                .registerInstances(Sets.newHashSet(
                    new ApiController(factory),
                    new ClusterCleanupController(cluster,factory),
                    new ClusterRepairController(cluster,factory),
                    new ClusterRollingRestartController(cluster,factory),
                    new ConfigController(cluster,factory),
                    new LiveEndpointsController(cluster,factory),
                    new NodeController(cluster,factory),
                    new QaReportController(cluster, factory)
                ));
            httpServer = GrizzlyHttpServerFactory.createHttpServer(httpServerBaseUri, rc);
            httpServer.start();

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup() {
        if (httpServer != null) {
            httpServer.shutdown();
        }
    }

}
