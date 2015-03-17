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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.mesosphere.mesos.util.Tuple2;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

public class ApiControllerTest extends AbstractSchedulerTest {
    private URI httpServerBaseUri;
    private HttpServer httpServer;

    @Test
    public void testRoot() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/");
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertEquals(resolve("/config").toString(), json.get("configuration").asText());
        assertEquals(resolve("/nodes").toString(), json.get("allNodes").asText());
        assertThat(json.get("repair")).isInstanceOf(ObjectNode.class);
        assertThat(json.get("cleanup")).isInstanceOf(ObjectNode.class);
    }

    @Test
    public void testConfig() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/config");
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertEquals("test-cluster", json.get("frameworkName").asText());
        assertEquals("", json.get("frameworkId").asText());
        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());
    }

    @Test
    public void testNodes() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/nodes");
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertTrue(json.has("nodes"));
    }

    @Test
    public void testLiveNodes() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/live-nodes?limit=2");

        // must return HTTP/500 if no nodes are present
        assertEquals(400, tup._1.intValue());

        // add one live node
        addNode("exec1", "1.2.3.4");

        tup = fetchJson("/live-nodes?limit=2");
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;
        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());
        assertEquals(7199, json.get("jmxPort").asInt());
        JsonNode nodes = json.get("liveNodes");
        assertEquals(JsonNodeType.ARRAY, nodes.getNodeType());
        assertThat(nodes)
            .hasSize(1)
            .contains(TextNode.valueOf("1.2.3.4"));

        // test more output formats

        Tuple2<Integer, String> str = fetchText("/live-nodes/cqlsh", "text/x-cassandra-cqlsh");
        assertEquals(200, str._1.intValue());
        assertEquals("1.2.3.4 9042", str._2);

        str = fetchText("/live-nodes/stress?limit=2", "text/x-cassandra-stress");
        assertEquals(200, str._1.intValue());
        assertEquals("-node 1.2.3.4 -port native=9042 thrift=9160 jmx=7199", str._2);

        str = fetchText("/live-nodes/nodetool", "text/x-cassandra-nodetool");
        assertEquals(200, str._1.intValue());
        assertEquals("-h 1.2.3.4 -p 7199", str._2);

        str = fetchText("/live-nodes/text?limit=2", "text/plain");
        assertEquals(200, str._1.intValue());
        assertEquals("9042\n1.2.3.4\n", str._2);

        str = fetchText("/live-nodes/cqlsh", "text/x-cassandra-cqlsh");
        assertEquals(200, str._1.intValue());
        assertEquals("1.2.3.4 9042", str._2);

        str = fetchText("/live-nodes/stress", "text/x-cassandra-stress");
        assertEquals(200, str._1.intValue());
        assertEquals("-node 1.2.3.4 -port native=9042 thrift=9160 jmx=7199", str._2);

        str = fetchText("/live-nodes/nodetool", "text/x-cassandra-nodetool");
        assertEquals(200, str._1.intValue());
        assertEquals("-h 1.2.3.4 -p 7199", str._2);

        str = fetchText("/live-nodes/text", "text/plain");
        assertEquals(200, str._1.intValue());
        assertEquals("9042\n1.2.3.4\n", str._2);

        //
        // mark node as dead
        //

        cluster.recordHealthCheck("exec1", healthCheckDetailsFailed());
        tup = fetchJson("/live-nodes?limit=2");
        assertEquals(400, tup._1.intValue());

        str = fetchText("/live-nodes/cqlsh", "text/x-cassandra-cqlsh");
        assertEquals(400, str._1.intValue());

        str = fetchText("/live-nodes/stress?limit=2", "text/x-cassandra-stress");
        assertEquals(400, str._1.intValue());

        str = fetchText("/live-nodes/nodetool", "text/x-cassandra-nodetool");
        assertEquals(400, str._1.intValue());

        str = fetchText("/live-nodes/text?limit=2", "text/plain");
        assertEquals(400, str._1.intValue());

        // add a live nodes

        addNode("exec2", "2.2.2.2");

        tup = fetchJson("/live-nodes?limit=2");
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());
        assertEquals(7199, json.get("jmxPort").asInt());
        nodes = json.get("liveNodes");
        assertEquals(JsonNodeType.ARRAY, nodes.getNodeType());
        assertThat(nodes)
            .hasSize(1)
            .contains(TextNode.valueOf("2.2.2.2"));

        str = fetchText("/live-nodes/cqlsh", "text/x-cassandra-cqlsh");
        assertEquals(200, str._1.intValue());
        assertEquals("2.2.2.2 9042", str._2);

        str = fetchText("/live-nodes/stress?limit=2", "text/x-cassandra-stress");
        assertEquals(200, str._1.intValue());
        assertEquals("-node 2.2.2.2 -port native=9042 thrift=9160 jmx=7199", str._2);

        str = fetchText("/live-nodes/nodetool", "text/x-cassandra-nodetool");
        assertEquals(200, str._1.intValue());
        assertEquals("-h 2.2.2.2 -p 7199", str._2);

        str = fetchText("/live-nodes/text?limit=2", "text/plain");
        assertEquals(200, str._1.intValue());
        assertEquals("9042\n2.2.2.2\n", str._2);

        // mark 1st node as live

        cluster.recordHealthCheck("exec1", healthCheckDetailsSuccess("NORMAL", true));

        tup = fetchJson("/live-nodes?limit=2");
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());
        assertEquals(7199, json.get("jmxPort").asInt());
        nodes = json.get("liveNodes");
        assertEquals(JsonNodeType.ARRAY, nodes.getNodeType());
        assertThat(nodes)
            .hasSize(2)
            .contains(TextNode.valueOf("1.2.3.4"))
            .contains(TextNode.valueOf("2.2.2.2"));

        str = fetchText("/live-nodes/cqlsh", "text/x-cassandra-cqlsh");
        assertEquals(200, str._1.intValue());

        str = fetchText("/live-nodes/stress?limit=2", "text/x-cassandra-stress");
        assertEquals(200, str._1.intValue());

        str = fetchText("/live-nodes/nodetool", "text/x-cassandra-nodetool");
        assertEquals(200, str._1.intValue());

        str = fetchText("/live-nodes/text?limit=2", "text/plain");
        assertEquals(200, str._1.intValue());
    }

    private void addNode(String executorId, String ip) {
        cluster.getClusterState().addOrSetNode(CassandraFrameworkProtos.CassandraNode.newBuilder()
            .setCassandraNodeExecutor(
                CassandraFrameworkProtos.CassandraNodeExecutor.newBuilder(CassandraFrameworkProtos.CassandraNodeExecutor.getDefaultInstance())
                    .setExecutorId(executorId)
                    .setSource("source")
                    .setCpuCores(1)
                    .setMemMb(1)
                    .setDiskMb(1)
                    .setCommand("comand")
            )
            .setHostname(ip)
            .setIp(ip)
            .setSeed(false)
            .setTargetRunState(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN)
            .setJmxConnect(CassandraFrameworkProtos.JmxConnect.newBuilder()
                .setIp(ip)
                .setJmxPort(7199))
            .addTasks(CassandraFrameworkProtos.CassandraNodeTask.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(executorId + ".server")
                .setTaskType(CassandraFrameworkProtos.CassandraNodeTask.TaskType.SERVER)
                .setCpuCores(1)
                .setMemMb(1)
                .setDiskMb(1))
            .build());
        cluster.recordHealthCheck(executorId, healthCheckDetailsSuccess("NORMAL", true));
    }

    @Test
    public void testNonExistingUri() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/does-not-exist");
        assertEquals(404, tup._1.intValue());
    }

    private Tuple2<Integer, JsonNode> fetchJson(String rel) throws Exception {
        JsonFactory factory = new JsonFactory();
        HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
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

    private Tuple2<Integer, String> fetchText(String rel, String expectedContentType) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
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
                while ((rd = in.read()) != -1)
                    sb.append((char) rd);
                return Tuple2.tuple2(responseCode, sb.toString());
            } finally {
                in.close();
            }
        } finally {
            conn.disconnect();
        }
    }

    @NotNull
    private URI resolve(String rel) {
        return httpServerBaseUri.resolve(rel);
    }

    @Before
    public void cleanState() {
        super.cleanState();

        try {
            try (ServerSocket sock = new ServerSocket(0)) {
                httpServerBaseUri = URI.create(String.format("http://%s:%d/", formatInetAddress(InetAddress.getLocalHost()), sock.getLocalPort()));
            }

            final ResourceConfig rc = new ResourceConfig()
                .register(new ApiController(cluster));
            httpServer = GrizzlyHttpServerFactory.createHttpServer(httpServerBaseUri, rc);
            httpServer.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void cleanup() {
        if (httpServer != null)
            httpServer.shutdown();
    }

    @NotNull
    private static String formatInetAddress(@NotNull final InetAddress inetAddress) {
        if (inetAddress instanceof Inet4Address) {
            final Inet4Address address = (Inet4Address) inetAddress;
            return address.getHostAddress();
        } else if (inetAddress instanceof Inet6Address) {
            final Inet6Address address = (Inet6Address) inetAddress;
            return String.format("[%s]", address.getHostAddress());
        } else {
            throw new IllegalArgumentException("InetAddress type: " + inetAddress.getClass().getName() + " is not supported");
        }
    }

}
