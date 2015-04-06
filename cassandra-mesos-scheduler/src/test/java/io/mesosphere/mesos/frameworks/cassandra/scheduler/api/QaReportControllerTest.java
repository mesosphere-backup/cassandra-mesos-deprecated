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
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QaReportControllerTest extends AbstractApiControllerTest {

    @Test
    public void testQaReportResources() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/qa/report/resources", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        for (int i = 0; i < activeNodes; i++) {
            Tuple2<Protos.SlaveID, String> slave = slaves[i];
            JsonNode node = json.get("nodes").get(executorMetadata[i].getExecutor().getExecutorId().getValue());
            assertEquals(slave._2, node.get("ip").asText());
            assertTrue(node.has("workdir"));
            assertTrue(node.has("hostname"));
            assertTrue(node.has("targetRunState"));
            assertTrue(node.has("jmxIp"));
            assertTrue(node.has("jmxPort"));
            assertTrue(node.has("live"));
            assertTrue(node.get("logfiles").isArray());
            assertEquals(2, node.get("logfiles").size());
        }
    }

    @Test
    public void testQaReportResources_text() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, String> tup = fetchText("/qa/report/resources", "text/plain");
        assertEquals(200, tup._1.intValue());

        try (BufferedReader br = new BufferedReader(new StringReader(tup._2))) {

            for (int i = 0; i < activeNodes; i++) {
                Tuple2<Protos.SlaveID, String> slave = slaves[i];
                assertThat(br.readLine()).startsWith("JMX_PORT: ");
                assertEquals("JMX_IP: 127.0.0.1", br.readLine());
                assertEquals("NODE_IP: " + slave._2, br.readLine());
                assertEquals("BASE: http://" + slave._2 + ":5051/", br.readLine());
                assertTrue(br.readLine().startsWith("LOG: "));
                assertTrue(br.readLine().startsWith("LOG: "));
            }
        }
    }
}
