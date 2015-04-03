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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.mesosphere.mesos.util.Tuple2;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ApiControllerTest extends AbstractApiControllerTest {

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
    public void testNonExistingUri() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/does-not-exist", false);
        assertEquals(404, tup._1.intValue());
    }

}
