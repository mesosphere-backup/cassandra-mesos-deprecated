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
import org.apache.mesos.Protos;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.*;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

public class ApiControllerTest extends AbstractCassandraSchedulerTest {
    private URI httpServerBaseUri;
    private HttpServer httpServer;

    @Test
    public void testSeedNodes() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/seed-nodes", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());

        JsonNode seeds = json.get("seeds");
        assertThat(seeds.isArray());
        assertEquals(2, seeds.size());
        assertEquals(slaves[0]._2, seeds.get(0).asText());
        assertEquals(slaves[1]._2, seeds.get(1).asText());

        // make node 2 non-seed

        tup = fetchJson("/node/non-seed/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertTrue(json.get("oldSeedState").asBoolean());
        assertTrue(json.get("success").asBoolean());
        assertFalse(json.get("seedState").asBoolean());
        assertFalse(json.has("error"));

        // verify

        tup = fetchJson("/seed-nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;

        seeds = json.get("seeds");
        assertThat(seeds.isArray());
        assertEquals(1, seeds.size());
        assertEquals(slaves[0]._2, seeds.get(0).asText());

        // make node 2 non-seed again

        tup = fetchJson("/node/non-seed/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertFalse(json.get("oldSeedState").asBoolean());
        assertFalse(json.get("success").asBoolean());
        assertFalse(json.get("seedState").asBoolean());
        assertFalse(json.has("error"));

        // non-existing

        tup = fetchJson("/node/non-seed/foobar", true);
        assertEquals(404, tup._1.intValue());

        // too few seeds

        tup = fetchJson("/node/non-seed/" + slaves[0]._2, true);
        assertEquals(400, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[0]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertTrue(json.get("oldSeedState").asBoolean());
        assertFalse(json.get("success").asBoolean());
        assertTrue(json.has("error"));
        assertTrue(json.get("error").isTextual());


    }

    @Test
    public void testRoot() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertEquals(resolve("/config").toString(), json.get("configuration").asText());
        assertEquals(resolve("/nodes").toString(), json.get("allNodes").asText());
        assertThat(json.get("repair")).isInstanceOf(ObjectNode.class);
        assertThat(json.get("cleanup")).isInstanceOf(ObjectNode.class);
    }

    @Test
    public void testConfig() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/config", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertEquals("test-cluster", json.get("frameworkName").asText());
        assertEquals("", json.get("frameworkId").asText());
        assertEquals(9042, json.get("nativePort").asInt());
        assertEquals(9160, json.get("rpcPort").asInt());
    }

    @Test
    public void testRepair() throws Exception {
        testClusterJob("repair", "repair", "cleanup");
    }

    @Test
    public void testCleanup() throws Exception {
        testClusterJob("cleanup", "cleanup", "repair");
    }

    private void testClusterJob(String urlPart, String name, String other) throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson('/' + urlPart + "/start", true);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertTrue(json.has("started"));
        assertTrue(json.get("started").asBoolean());

        // fail for cleanup

        tup = fetchJson('/' + other + "/start", true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;

        assertTrue(json.has("started"));
        assertFalse(json.get("started").asBoolean());

        // status

        tup = fetchJson('/' + urlPart + "/status", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertTrue(json.get("running").asBoolean());
        JsonNode status = json.get(name);
        assertTrue(status.has("type"));
        assertTrue(status.has("started"));
        assertTrue(status.get("started").isNumber());
        assertTrue(status.has("finished"));
        assertTrue(status.get("finished").isNull());
        assertTrue(status.has("aborted"));
        assertFalse(status.get("aborted").asBoolean());
        assertTrue(status.has("remainingNodes"));
        assertTrue(status.get("remainingNodes").isArray());
        assertTrue(status.has("currentNode"));
        assertTrue(status.has("completedNodes"));
        assertTrue(status.get("completedNodes").isArray());

        // abort

        tup = fetchJson('/' + urlPart + "/abort", true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertTrue(json.get("aborted").asBoolean());

        tup = fetchJson('/' + urlPart + "/status", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        status = json.get(name);
        assertTrue(status.has("aborted"));
        assertTrue(status.get("aborted").asBoolean());

        // last

        tup = fetchJson('/' + urlPart + "/last", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertFalse(json.get("present").asBoolean());
        assertTrue(json.get(name).isNull());
    }

    @Test
    public void testNodeTargetState() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;
        assertEquals("RUN", json.get("nodes").get(1).get("targetRunState").asText());

        //

        tup = fetchJson("/node/stop/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("STOP", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("STOP", json.get("nodes").get(1).get("targetRunState").asText());

        //

        tup = fetchJson("/node/restart/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("RESTART", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("RESTART", json.get("nodes").get(1).get("targetRunState").asText());

        //

        tup = fetchJson("/node/run/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("RUN", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("RUN", json.get("nodes").get(1).get("targetRunState").asText());

        //

        tup = fetchJson("/node/terminate/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("TERMINATE", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("TERMINATE", json.get("nodes").get(1).get("targetRunState").asText());
    }

    @Test
    public void testReplaceNode() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/node/terminate/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;
        assertEquals(slaves[1]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("TERMINATE", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("TERMINATE", json.get("nodes").get(1).get("targetRunState").asText());

        //

        tup = fetchJson("/node/replace/" + slaves[1]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[1]._2, json.get("ipToReplace").asText());
        assertFalse(json.get("success").asBoolean());
        assertTrue(json.has("reason"));
        assertTrue(json.get("reason").isTextual());

        //

        tup = fetchJson("/node/terminate/" + slaves[2]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[2]._2, json.get("ip").asText());
        assertTrue(json.has("hostname"));
        assertEquals("TERMINATE", json.get("targetRunState").asText());

        tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals("TERMINATE", json.get("nodes").get(2).get("targetRunState").asText());

        //

        tup = fetchJson("/node/replace/" + slaves[2]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[2]._2, json.get("ipToReplace").asText());
        assertFalse(json.get("success").asBoolean());
        assertTrue(json.has("reason"));
        assertTrue(json.get("reason").isTextual());

        //

        cluster.removeExecutor(executorMetadata[2].getExecutor().getExecutorId().getValue());

        tup = fetchJson("/node/replace/" + slaves[2]._2, true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertEquals(slaves[2]._2, json.get("ipToReplace").asText());
        assertTrue(json.get("success").asBoolean());

    }

    @Test
    public void testQaReportResources() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/qaReportResources", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertTrue(json.get("jmxPort").isNumber());

        for (int i = 0; i < activeNodes; i++) {
            Tuple2<Protos.SlaveID, String> slave = slaves[i];
            JsonNode node = json.get("nodes").get(executorMetadata[i].getExecutor().getExecutorId().getValue());
            assertEquals(slave._2, node.get("ip").asText());
            assertTrue(node.has("workdir"));
            assertTrue(node.has("hostname"));
            assertTrue(node.has("targetRunState"));
            assertTrue(node.has("jmxPort"));
            assertTrue(node.has("live"));
            assertTrue(node.get("logfiles").isArray());
            assertEquals(2, node.get("logfiles").size());
        }
    }

    @Test
    public void testQaReportResourcesText() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, String> tup = fetchText("/qaReportResources/text", "text/plain");
        assertEquals(200, tup._1.intValue());

        try (BufferedReader br = new BufferedReader(new StringReader(tup._2))) {
            assertTrue(br.readLine().startsWith("JMX: "));

            for (int i = 0; i < activeNodes; i++) {
                Tuple2<Protos.SlaveID, String> slave = slaves[i];
                assertEquals("IP: " + slave._2, br.readLine());
                assertEquals("BASE: http://" + slave._2 + ":5051/", br.readLine());
                assertTrue(br.readLine().startsWith("LOG: "));
                assertTrue(br.readLine().startsWith("LOG: "));
            }
        }
    }

    @Test
    public void testNodes() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/nodes", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertTrue(json.has("nodes"));
        JsonNode nodes = json.get("nodes");
        assertEquals(3, nodes.size());

        for (int i = 0; i < 3; i++) {
            JsonNode node = nodes.get(i);
            assertEquals(slaves[i]._2, node.get("ip").asText());
        }

        JsonNode node = nodes.get(0);
        assertEquals(executorMetadata[0].getExecutor().getExecutorId().getValue(), node.get("executorId").asText());
        assertEquals("/foo/bar/baz", node.get("workdir").asText());
        assertTrue(node.has("hostname"));
        assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN.name(), node.get("targetRunState").asText());
        assertTrue(node.get("jmxPort").isNumber());
        assertTrue(node.get("seedNode").asBoolean());
        assertTrue(node.get("cassandraDaemonPid").isNull());

        JsonNode tasks = node.get("tasks");
        assertNotNull(tasks);
        JsonNode task = tasks.get("SERVER");
        assertNotNull(task);
        assertFalse(task.isNull());
        assertTrue(task.has("cpuCores"));
        assertTrue(task.has("diskMb"));
        assertTrue(task.has("memMb"));
        assertTrue(task.has("taskId"));

        assertTrue(node.get("lastHealthCheck").isNumber());
        JsonNode hcd = node.get("healthCheckDetails");
        assertFalse(hcd.isNull());
        assertTrue(hcd.has("healthy"));
        assertTrue(hcd.has("msg"));
        assertTrue(hcd.has("version"));
        assertTrue(hcd.has("operationMode"));
        assertTrue(hcd.has("clusterName"));
        assertTrue(hcd.has("dataCenter"));
        assertTrue(hcd.has("rack"));
        assertTrue(hcd.has("endpoint"));
        assertTrue(hcd.has("hostId"));
        assertTrue(hcd.has("joined"));
        assertTrue(hcd.has("gossipInitialized"));
        assertTrue(hcd.has("gossipRunning"));
        assertTrue(hcd.has("nativeTransportRunning"));
        assertTrue(hcd.has("rpcServerRunning"));
        assertTrue(hcd.has("tokenCount"));
        assertTrue(hcd.has("uptimeMillis"));
    }

    @Test
    public void testLiveNodes() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/live-nodes?limit=2", false);

        // must return HTTP/500 if no nodes are present
        assertEquals(400, tup._1.intValue());

        // add one live node
        addNode("exec1", "1.2.3.4");

        tup = fetchJson("/live-nodes?limit=2", false);
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
        assertEquals("NATIVE: 9042\n" +
            "RPC: 9160\n" +
            "JMX: 7199\n" +
            "IP: 1.2.3.4\n", str._2);

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
        assertEquals("NATIVE: 9042\n" +
            "RPC: 9160\n" +
            "JMX: 7199\n" +
            "IP: 1.2.3.4\n", str._2);

        //
        // mark node as dead
        //

        cluster.recordHealthCheck("exec1", healthCheckDetailsFailed());
        tup = fetchJson("/live-nodes?limit=2", false);
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

        tup = fetchJson("/live-nodes?limit=2", false);
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
        assertEquals("NATIVE: 9042\n" +
            "RPC: 9160\n" +
            "JMX: 7199\n" +
            "IP: 2.2.2.2\n", str._2);

        // mark 1st node as live

        cluster.recordHealthCheck("exec1", healthCheckDetailsSuccess("NORMAL", true));

        tup = fetchJson("/live-nodes?limit=2", false);
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

    @Test
    public void testNonExistingUri() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/does-not-exist", false);
        assertEquals(404, tup._1.intValue());
    }

    private Tuple2<Integer, JsonNode> fetchJson(String rel, boolean post) throws Exception {
        JsonFactory factory = new JsonFactory();
        HttpURLConnection conn = (HttpURLConnection) resolve(rel).toURL().openConnection();
        try {
            conn.setRequestMethod(post ? "POST" : "GET");
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
    private URI resolve(String rel) {
        return httpServerBaseUri.resolve(rel);
    }

    @Before
    public void cleanState() {
        super.cleanState();

        try {
            try (ServerSocket sock = new ServerSocket(0)) {
                httpServerBaseUri = URI.create(String.format("http://%s:%d/", formatInetAddress(InetAddress.getLoopbackAddress()), sock.getLocalPort()));
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
        if (httpServer != null) {
            httpServer.shutdown();
        }
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
