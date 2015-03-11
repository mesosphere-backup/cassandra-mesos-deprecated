/**
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
package io.mesosphere.mesos.frameworks.cassandra.javadriver;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

@Path("/")
public class TestController {
    @GET
    @Path("/live-nodes")
    @Produces("application/json")
    public Response twoNodes(@QueryParam("limit") @DefaultValue("3") int limit) {
        return Response.ok("{\n" +
            "  \"clusterName\" : \"Foo Cluster\",\n" +
            "  \"nativePort\" : 111,\n" +
            "  \"rpcPort\" : 222,\n" +
            "  \"jmxPort\" : 333,\n" +
            "  \"liveNodes\" : [ \"127.0.0.2\", \"127.0.0.1\" ]\n" +
            "}\n", "application/json").build();
    }
}
