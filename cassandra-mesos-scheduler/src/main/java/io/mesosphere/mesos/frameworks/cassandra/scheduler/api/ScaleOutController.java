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
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.NodeCounts;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/scale")
public final class ScaleOutController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public ScaleOutController(@NotNull CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Allows to scale out the Cassandra cluster by increasing the number of nodes.
     * Requires the query parameter {@code nodes} defining the desired number of total nodes.
     * Must be submitted using HTTP method {@code POST}.
     */
    @POST
    @Path("/nodes")
    public Response updateNodeCount(@QueryParam("nodes") final int nodeCount) {
        final NodeCounts oldNodeCount = cluster.getClusterState().nodeCounts();
        final int newCount = cluster.updateNodeCount(nodeCount);
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeNumberField("oldNodeCount", oldNodeCount.getNodeCount());
                json.writeNumberField("seedNodeCount", oldNodeCount.getSeedCount());
                json.writeBooleanField("applied", newCount == nodeCount);
                json.writeNumberField("newNodeCount", newCount);
            }
        });
    }
}
