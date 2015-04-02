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
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.SeedChangeException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/")
public final class SeedNodesController extends AbstractApiController {

    public SeedNodesController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Returns a JSON with the IP addresses of all seed nodes and native, thrift and JMX port numbers.
     *
     * Example:
     * <pre>{@code {
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "jmxPort" : 7199,
     * "seeds" : [ "127.0.0.1" ]
     * }}</pre>
     */
    @GET
    @Path("/seed-nodes")
    public Response seedNodes() {
        final CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {

                json.writeNumberField("nativePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
                json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
                json.writeNumberField("jmxPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX));

                writeSeedIps(json);
            }
        });
    }

    /**
     * Allows to make a non-seed node a seed node. The node is specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * Must be submitted using HTTP method {@code POST}.
     *
     * Example: <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "false",
     * "success" : "false",
     * "error" : "Some error message"
     * }}</pre>
     *
     * <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "false",
     * "success" : "true",
     * "seedState" : "true"
     * }}</pre>
     */
    @POST
    @Path("/node/seed/{node}")
    public Response nodeMakeSeed(@PathParam("node") String node) {
        return nodeUpdateSeed(node, true);
    }

    /**
     * Allows to make a seed node a non-seed node. The node is specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * Must be submitted using HTTP method {@code POST}.
     *
     * Example: <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "true",
     * "success" : "false",
     * "error" : "Some error message"
     * }}</pre>
     *
     * <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "true",
     * "success" : "true",
     * "seedState" : "false"
     * }}</pre>
     */
    @POST
    @Path("/node/non-seed/{node}")
    public Response nodeMakeNonSeed(@PathParam("node") String node) {
        return nodeUpdateSeed(node, false);
    }

    private Response nodeUpdateSeed(String node, final boolean seed) {
        final CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);
        if (cassandraNode == null) {
            return Response.status(404).build();
        }

        try {
            final boolean seedChanged = cluster.setNodeSeed(cassandraNode, seed);

            return buildStreamingResponse(new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    }
                    json.writeBooleanField("oldSeedState", cassandraNode.getSeed());

                    if (seedChanged) {
                        json.writeBooleanField("success", true);
                        json.writeBooleanField("seedState", seed);
                    } else {
                        json.writeBooleanField("success", false);
                        json.writeBooleanField("seedState", cassandraNode.getSeed());
                    }

                }
            });
        } catch (final SeedChangeException e) {
            return buildStreamingResponse(Response.Status.BAD_REQUEST, new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    }
                    json.writeBooleanField("oldSeedState", cassandraNode.getSeed());

                    json.writeBooleanField("success", false);
                    json.writeStringField("error", e.getMessage());
                }
            });
        }

    }
}
