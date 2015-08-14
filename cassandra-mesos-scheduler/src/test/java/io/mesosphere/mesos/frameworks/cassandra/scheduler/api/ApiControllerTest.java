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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.api.ApiController.ApiEndpoint;
import io.mesosphere.mesos.util.Tuple2;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ApiControllerTest extends AbstractApiControllerTest {

    public static final TypeReference<List<ApiEndpoint>> LIST_TYPE_REFERENCE = new TypeReference<List<ApiEndpoint>>() {
    };

    @Test
    public void testRoot() throws Exception {
        final Tuple2<Integer, JsonNode> tup = fetchJson("/", false);
        assertEquals(200, tup._1.intValue());
        final JsonNode json = tup._2;
        final String baseUri = resolve("/").toString();

        final ObjectMapper mapper = new ObjectMapper();
        final List<ApiEndpoint> list = mapper.readValue(json.toString(), LIST_TYPE_REFERENCE);

        assertThat(list).isEqualTo(
                newArrayList(
                        new ApiEndpoint("GET",  "config", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/backup/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/backup/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/backup/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/backup/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/cleanup/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/cleanup/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/cleanup/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/cleanup/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/repair/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/repair/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/repair/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/repair/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/restore/start?name=$name", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/restore/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/restore/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/restore/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/rolling-restart/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", "cluster/rolling-restart/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/rolling-restart/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "cluster/rolling-restart/last", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "node/all", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "node/seed/all", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/stop/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/start/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/restart/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/terminate/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/replace/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/rackdc", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/make-seed/", newArrayList("application/json")),
                        new ApiEndpoint("POST", "node/{node}/make-non-seed/", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "live-nodes", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "live-nodes/text", newArrayList("text/plain")),
                        new ApiEndpoint("GET",  "live-nodes/cqlsh", newArrayList("text/x-cassandra-cqlsh")),
                        new ApiEndpoint("GET",  "live-nodes/nodetool", newArrayList("text/x-cassandra-nodetool")),
                        new ApiEndpoint("GET",  "live-nodes/stress", newArrayList("text/x-cassandra-stress")),
                        new ApiEndpoint("GET",  "qa/report/resources", newArrayList("application/json", "text/plain")),
                        new ApiEndpoint("POST", "scale/nodes?nodeCount={nodeCount}", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "health/process", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "health/cluster", newArrayList("application/json")),
                        new ApiEndpoint("GET",  "health/cluster/report", newArrayList("application/json"))
                )
        );
    }

    @Test
    public void testNonExistingUri() throws Exception {
        final Tuple2<Integer, JsonNode> tup = fetchJson("/does-not-exist", false);
        assertEquals(404, tup._1.intValue());
    }

}
