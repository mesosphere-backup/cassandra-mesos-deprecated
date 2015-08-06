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

import static org.junit.Assert.*;

public class ClusterRestoreControllerTest extends AbstractApiControllerTest {

    @Test
    public void testRestore() throws Exception {
        threeNodeCluster();

        Tuple2<Integer, JsonNode> tup = fetchJson("/cluster/restore/start?name=backup", true);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;

        assertTrue(json.has("started"));
        assertTrue(json.get("started").asBoolean());

        // fail for restore

        tup = fetchJson("/cluster/restore/start?name=backup", true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;

        assertTrue(json.has("started"));
        assertFalse(json.get("started").asBoolean());

        // status

        tup = fetchJson("/cluster/restore/status", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertTrue(json.get("running").asBoolean());
        JsonNode status = json.get("restore");
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
        assertEquals("backup", status.get("backupName").asText());

        // abort

        tup = fetchJson("/cluster/restore/abort", true);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertTrue(json.get("aborted").asBoolean());

        tup = fetchJson("/cluster/restore/status", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        status = json.get("restore");
        assertTrue(status.has("aborted"));
        assertTrue(status.get("aborted").asBoolean());

        // last

        tup = fetchJson("/cluster/restore/last", false);
        assertEquals(200, tup._1.intValue());
        json = tup._2;
        assertFalse(json.get("present").asBoolean());
        assertTrue(json.get("restore").isNull());
    }
}
