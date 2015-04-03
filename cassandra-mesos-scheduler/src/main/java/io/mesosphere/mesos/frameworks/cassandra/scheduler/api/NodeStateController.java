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
import io.mesosphere.mesos.frameworks.cassandra.scheduler.ReplaceNodePreconditionFailed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.StringWriter;

@Path("/")
public final class NodeStateController extends AbstractApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeStateController.class);

    public NodeStateController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Sets requested state of the Cassandra process to 'stop' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/stop/{node}")
    public Response nodeStop(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeStop(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'run' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/run/{node}")
    public Response nodeStart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRun(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'restart' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/restart/{node}")
    public Response nodeRestart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRestart(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'terminate' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     * Note that a terminated node cannot be restarted.
     */
    @POST
    @Path("/node/terminate/{node}")
    public Response nodeTerminate(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeTerminate(node);

        return nodeStatusUpdate(cassandraNode);
    }

    private static Response nodeStatusUpdate(CassandraFrameworkProtos.CassandraNode cassandraNode) {

        int status = 200;
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            if (cassandraNode == null) {
                status = 404;
            } else {
                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    json.writeNullField("executorId");
                } else {
                    json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                }
                json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
            }

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.status(status).entity(sw.toString()).type("application/json").build();
    }

    /**
     * Submit intent to replace the already terminated node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/replace/{node}")
    public Response nodeReplace(@PathParam("node") String node) {
        int status = 200;
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);

            if (cassandraNode == null) {
                status = 404;
                json.writeBooleanField("success", false);
                json.writeStringField("reason", "No such node");
            } else {
                json.writeStringField("ipToReplace", cassandraNode.getIp());
                try {
                    cluster.replaceNode(node);

                    json.writeBooleanField("success", true);
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                } catch (ReplaceNodePreconditionFailed e) {
                    json.writeBooleanField("success", false);
                    json.writeStringField("reason", e.getMessage());
                }
            }

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.status(status).entity(sw.toString()).type("application/json").build();
    }
}
