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
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.NodeCounts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.StringWriter;

@Path("/")
public final class ScaleOutController extends AbstractApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScaleOutController.class);

    public ScaleOutController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Allows to scale out the Cassandra cluster by increasing the number of nodes.
     * Requires the query parameter {@code nodes} defining the desired number of total nodes.
     * Must be submitted using HTTP method {@code POST}.
     */
    @POST
    @Path("/scale/nodes")
    public Response updateNodeCount(@QueryParam("nodes") int nodeCount) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            NodeCounts oldNodeCount = cluster.getClusterState().nodeCounts();
            json.writeNumberField("oldNodeCount", oldNodeCount.getNodeCount());
            json.writeNumberField("seedNodeCount", oldNodeCount.getSeedCount());
            int newCount = cluster.updateNodeCount(nodeCount);
            json.writeBooleanField("applied", newCount == nodeCount);
            json.writeNumberField("newNodeCount", newCount);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }
}
