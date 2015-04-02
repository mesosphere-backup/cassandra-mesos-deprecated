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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class SeedNodesControllerTest extends AbstractApiControllerTest {

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
}
