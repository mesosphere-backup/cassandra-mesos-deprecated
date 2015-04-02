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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import io.mesosphere.mesos.util.Tuple2;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class LiveEndpointsControllerTest extends AbstractApiControllerTest {

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
}
