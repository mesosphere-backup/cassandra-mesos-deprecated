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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.StringWriter;

@Path("/")
public final class ApiController extends AbstractApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiController.class);

    public ApiController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Basially a poor man's index page.
     */
    @GET
    public Response indexPage(@Context UriInfo uriInfo) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

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

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to JSON doc", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }
}
