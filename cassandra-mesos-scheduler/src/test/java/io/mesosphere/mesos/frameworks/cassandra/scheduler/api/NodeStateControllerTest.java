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

import com.fasterxml.jackson.databind.JsonNode;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class NodeStateControllerTest extends AbstractApiControllerTest {

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
        Assert.assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN.name(), node.get("targetRunState").asText());
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
}
