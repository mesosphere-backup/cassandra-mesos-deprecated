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

import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/")
public final class ApiController extends AbstractApiController {

    public ApiController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Basially a poor man's index page.
     */
    @GET
    public Response indexPage(@Context final UriInfo uriInfo) {
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                String baseUrl = uriInfo.getBaseUri().toString();

                json.writeStringField("configuration", baseUrl + "config");
                json.writeStringField("seedNodes", baseUrl + "seed-nodes");
                json.writeStringField("allNodes", baseUrl + "nodes");

                json.writeObjectFieldStart("repair");
                json.writeStringField("start", baseUrl + "repair/start");
                json.writeStringField("status", baseUrl + "repair/status");
                json.writeStringField("lastStatus", baseUrl + "repair/last");
                json.writeStringField("abort", baseUrl + "repair/abort");
                json.writeEndObject();

                json.writeObjectFieldStart("cleanup");
                json.writeStringField("start", baseUrl + "cleanup/start");
                json.writeStringField("status", baseUrl + "cleanup/status");
                json.writeStringField("lastStatus", baseUrl + "cleanup/last");
                json.writeStringField("abort", baseUrl + "cleanup/abort");
                json.writeEndObject();
            }
        });
    }
}
