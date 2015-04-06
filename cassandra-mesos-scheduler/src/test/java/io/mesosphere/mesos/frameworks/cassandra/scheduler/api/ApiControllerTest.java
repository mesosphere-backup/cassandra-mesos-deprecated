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
        Tuple2<Integer, JsonNode> tup = fetchJson("/", false);
        assertEquals(200, tup._1.intValue());
        JsonNode json = tup._2;
        final String baseUri = resolve("/").toString();

        final ObjectMapper mapper = new ObjectMapper();
        final List<ApiEndpoint> list = mapper.readValue(json.toString(), LIST_TYPE_REFERENCE);

        assertThat(list).hasSize(28);
        assertThat(list).isEqualTo(
                newArrayList(
                        new ApiEndpoint("GET",  baseUri + "config", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/cleanup/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/cleanup/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/cleanup/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/cleanup/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/repair/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/repair/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/repair/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/repair/last", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/rolling-restart/start", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "cluster/rolling-restart/abort", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/rolling-restart/status", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "cluster/rolling-restart/last", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "node/all", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "node/seed/all", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/stop/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/start/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/restart/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/terminate/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/replace/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/make-seed/", newArrayList("application/json")),
                        new ApiEndpoint("POST", baseUri + "node/{node}/make-non-seed/", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "live-nodes", newArrayList("application/json")),
                        new ApiEndpoint("GET",  baseUri + "live-nodes/text", newArrayList("text/plain")),
                        new ApiEndpoint("GET",  baseUri + "live-nodes/cqlsh", newArrayList("text/x-cassandra-cqlsh")),
                        new ApiEndpoint("GET",  baseUri + "live-nodes/nodetool", newArrayList("text/x-cassandra-nodetool")),
                        new ApiEndpoint("GET",  baseUri + "live-nodes/stress", newArrayList("text/x-cassandra-stress")),
                        new ApiEndpoint("GET",  baseUri + "qa/report/resources", newArrayList("application/json", "text/plain"))
                )
        );
    }

    @Test
    public void testNonExistingUri() throws Exception {
        Tuple2<Integer, JsonNode> tup = fetchJson("/does-not-exist", false);
        assertEquals(404, tup._1.intValue());
    }

}
