package io.mesosphere.mesos.frameworks.cassandra;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/")
public final class ApiController {

    @GET
    @Path("/hi")
    @Produces("text/plain")
    public String hi() {
        return "Hello World";
    }

}
