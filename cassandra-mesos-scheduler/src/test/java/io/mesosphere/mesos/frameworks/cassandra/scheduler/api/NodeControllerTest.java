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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class NodeControllerTest extends AbstractApiControllerTest {

    @Test
    public void testNodeTargetState() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        JsonNode json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("RUN");

        //

        tup = postJson(String.format("/node/%s/stop", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("STOP");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("STOP");

        //

        tup = postJson(String.format("/node/%s/restart", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("RESTART");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("RESTART");

        //

        tup = postJson(String.format("/node/%s/start", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("RUN");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("RUN");

        //

        tup = postJson(String.format("/node/%s/terminate", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("TERMINATE");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("TERMINATE");
    }

    @Test
    public void testReplaceNode() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = postJson(String.format("/node/%s/terminate", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        JsonNode json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("TERMINATE");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(1).get("targetRunState").asText()).isEqualTo("TERMINATE");

        //

        tup = postJson(String.format("/node/%s/replace", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(400);
        json = tup._2;
        assertThat(json.get("ipToReplace").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.get("success").asBoolean()).isFalse();
        assertThat(json.has("reason")).isTrue();
        assertThat(json.get("reason").isTextual()).isTrue();

        //

        tup = postJson(String.format("/node/%s/terminate", slaves[2]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[2]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("targetRunState").asText()).isEqualTo("TERMINATE");

        tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("nodes").get(2).get("targetRunState").asText()).isEqualTo("TERMINATE");

        //

        tup = postJson(String.format("/node/%s/replace", slaves[2]._2));
        assertThat(tup._1.intValue()).isEqualTo(400);
        json = tup._2;
        assertThat(json.get("ipToReplace").asText()).isEqualTo(slaves[2]._2);
        assertThat(json.get("success").asBoolean()).isFalse();
        assertThat(json.has("reason")).isTrue();
        assertThat(json.get("reason").isTextual()).isTrue();

        //

        cluster.removeExecutor(executorMetadata[2].getExecutor().getExecutorId().getValue());

        tup = postJson(String.format("/node/%s/replace", slaves[2]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ipToReplace").asText()).isEqualTo(slaves[2]._2);
        assertThat(json.get("success").asBoolean()).isTrue();
    }

    @Test
    public void testNodes() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = getJson("/node/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        JsonNode json = tup._2;

        assertThat(json.has("nodes")).isTrue();
        JsonNode nodes = json.get("nodes");
        assertThat(nodes.size()).isEqualTo(3);

        for (int i = 0; i < 3; i++) {
            JsonNode node = nodes.get(i);
            assertThat(node.get("ip").asText()).isEqualTo(slaves[i]._2);
        }

        JsonNode node = nodes.get(0);
        assertThat(node.get("executorId").asText()).isEqualTo(executorMetadata[0].getExecutor().getExecutorId().getValue());
        assertThat(node.get("workdir").asText()).isEqualTo("/foo/bar/baz");
        assertThat(node.has("hostname")).isTrue();
        assertThat(node.get("targetRunState").asText()).isEqualTo(CassandraFrameworkProtos.CassandraNode.TargetRunState.RUN.name());
        assertThat(node.get("jmxPort").isNumber()).isTrue();
        assertThat(node.get("seedNode").asBoolean()).isTrue();
        assertThat(node.get("cassandraDaemonPid").isNull()).isTrue();

        JsonNode tasks = node.get("tasks");
        assertNotNull(tasks);
        JsonNode task = tasks.get("SERVER");
        assertNotNull(task);
        assertThat(task.isNull()).isFalse();
        assertThat(task.has("cpuCores")).isTrue();
        assertThat(task.has("diskMb")).isTrue();
        assertThat(task.has("memMb")).isTrue();
        assertThat(task.has("taskId")).isTrue();

        assertThat(node.get("lastHealthCheck").isNumber()).isTrue();
        JsonNode hcd = node.get("healthCheckDetails");
        assertThat(hcd.isNull()).isFalse();
        assertThat(hcd.has("healthy")).isTrue();
        assertThat(hcd.has("msg")).isTrue();
        assertThat(hcd.has("version")).isTrue();
        assertThat(hcd.has("operationMode")).isTrue();
        assertThat(hcd.has("clusterName")).isTrue();
        assertThat(hcd.has("dataCenter")).isTrue();
        assertThat(hcd.has("rack")).isTrue();
        assertThat(hcd.has("endpoint")).isTrue();
        assertThat(hcd.has("hostId")).isTrue();
        assertThat(hcd.has("joined")).isTrue();
        assertThat(hcd.has("gossipInitialized")).isTrue();
        assertThat(hcd.has("gossipRunning")).isTrue();
        assertThat(hcd.has("nativeTransportRunning")).isTrue();
        assertThat(hcd.has("rpcServerRunning")).isTrue();
        assertThat(hcd.has("tokenCount")).isTrue();
        assertThat(hcd.has("uptimeMillis")).isTrue();
    }

    @Test
    public void testSeedNodes() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = getJson("/node/seed/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        JsonNode json = tup._2;

        assertThat(json.get("nativePort").asInt()).isEqualTo(9042);
        assertThat(json.get("rpcPort").asInt()).isEqualTo(9160);

        JsonNode seeds = json.get("seeds");
        assertThat(seeds.isArray());
        assertThat(seeds.size()).isEqualTo(2);
        assertThat(seeds.get(0).asText()).isEqualTo(slaves[0]._2);
        assertThat(seeds.get(1).asText()).isEqualTo(slaves[1]._2);

        // make node 2 non-seed

        tup = postJson(String.format("/node/%s/make-non-seed", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("oldSeedState").asBoolean()).isTrue();
        assertThat(json.get("success").asBoolean()).isTrue();
        assertThat(json.get("seedState").asBoolean()).isFalse();
        assertThat(json.has("error")).isFalse();

        // verify

        tup = getJson("/node/seed/all");
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;

        seeds = json.get("seeds");
        assertThat(seeds.isArray());
        assertThat(seeds.size()).isEqualTo(1);
        assertThat(seeds.get(0).asText()).isEqualTo(slaves[0]._2);

        // make node 2 non-seed again

        tup = postJson(String.format("/node/%s/make-non-seed", slaves[1]._2));
        assertThat(tup._1.intValue()).isEqualTo(200);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[1]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("oldSeedState").asBoolean()).isFalse();
        assertThat(json.get("success").asBoolean()).isFalse();
        assertThat(json.get("seedState").asBoolean()).isFalse();
        assertThat(json.has("error")).isFalse();

        // non-existing

        tup = postJson("/node/foobar/make-non-seed");
        assertThat(tup._1.intValue()).isEqualTo(404);

        // too few seeds

        tup = postJson(String.format("/node/%s/make-non-seed", slaves[0]._2));
        assertThat(tup._1.intValue()).isEqualTo(400);
        json = tup._2;
        assertThat(json.get("ip").asText()).isEqualTo(slaves[0]._2);
        assertThat(json.has("hostname")).isTrue();
        assertThat(json.get("oldSeedState").asBoolean()).isTrue();
        assertThat(json.get("success").asBoolean()).isFalse();
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").isTextual()).isTrue();
    }

}
