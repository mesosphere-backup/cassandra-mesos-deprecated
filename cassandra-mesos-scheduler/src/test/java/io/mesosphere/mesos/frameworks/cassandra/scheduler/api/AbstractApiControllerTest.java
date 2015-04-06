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
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.AbstractCassandraSchedulerTest;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.InetAddressUtils;
import io.mesosphere.mesos.util.Tuple2;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;

import static org.junit.Assert.assertEquals;

public abstract class AbstractApiControllerTest extends AbstractCassandraSchedulerTest {
    private final JsonFactory factory = new JsonFactory();
    protected URI httpServerBaseUri;
    protected HttpServer httpServer;

    protected void addNode(String executorId, String ip) {
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

    protected Tuple2<Integer, JsonNode> getJson(@NotNull String rel) throws Exception {
        return fetchJson(rel, false);
    }

    protected Tuple2<Integer, JsonNode> postJson(@NotNull String rel) throws Exception {
        return fetchJson(rel, true);
    }

    protected Tuple2<Integer, JsonNode> fetchJson(String rel, boolean post) throws Exception {
        JsonFactory factory = new JsonFactory();
        HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
            conn.setRequestMethod(post ? "POST" : "GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.connect();

            int responseCode = conn.getResponseCode();

            InputStream in;
            try {
                in = conn.getInputStream();
            } catch (IOException e) {
                in = conn.getErrorStream();
            }
            if (in == null) {
                return Tuple2.tuple2(responseCode, (JsonNode) MissingNode.getInstance());
            }

            assertEquals("application/json", conn.getHeaderField("Content-Type"));

            try {
                ObjectMapper om = new ObjectMapper();
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

    protected Tuple2<Integer, String> fetchText(String rel, String expectedContentType) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
            conn.setRequestProperty("Accept", expectedContentType);
            conn.connect();

            int responseCode = conn.getResponseCode();

            InputStream in;
            try {
                in = conn.getInputStream();
            } catch (IOException e) {
                in = conn.getErrorStream();
            }
            if (in == null) {
                return Tuple2.tuple2(responseCode, "");
            }

            assertEquals(expectedContentType, conn.getHeaderField("Content-Type"));

            try {
                StringBuilder sb = new StringBuilder();
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
    protected URI resolve(String rel) {
        return httpServerBaseUri.resolve(rel);
    }

    @Before
    public void cleanState() {
        super.cleanState();

        try {
            try (ServerSocket sock = new ServerSocket(0)) {
                httpServerBaseUri = URI.create(String.format("http://%s:%d/", InetAddressUtils.formatInetAddress(InetAddress.getLoopbackAddress()), sock.getLocalPort()));
            }

            final ResourceConfig rc = new ResourceConfig()
                .registerInstances(ApiControllerFactory.buildInstancesWithoutFiles(cluster, factory));
            httpServer = GrizzlyHttpServerFactory.createHttpServer(httpServerBaseUri, rc);
            httpServer.start();

        } catch (Exception e) {
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
