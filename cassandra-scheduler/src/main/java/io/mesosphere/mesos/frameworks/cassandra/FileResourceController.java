package io.mesosphere.mesos.frameworks.cassandra;

import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.File;

import static io.mesosphere.mesos.frameworks.cassandra.util.Env.workingDir;

@Path("/")
public final class FileResourceController {

    @NotNull
    private final File cassandraExecutorFile;
    @NotNull
    private final File jdkTarFile;
    @NotNull
    private final File cassandraTarFile;

    public FileResourceController() {
        cassandraExecutorFile = verifyFileExistsAndCanRead(Env.option("EXECUTOR_FILE_PATH").or(workingDir("/cassandra-executor.jar")));
        jdkTarFile = verifyFileExistsAndCanRead(Env.option("JDK_FILE_PATH").or(workingDir("/jdk-7u75-linux-x64.tar.gz")));
        cassandraTarFile = verifyFileExistsAndCanRead(Env.option("CASSANDRA_FILE_PATH").or(workingDir("/cassandra.tar.gz")));
    }

    @GET
    @Path("/cassandra-executor.jar")
    public Response cassandraExecutorJar() {
        return handleRequest(cassandraExecutorFile, "application/java-archive", "cassandra-executor.jar");
    }

    @GET
    @Path("/jdk.tar.gz")
    public Response jdkTar() {
        return handleRequest(jdkTarFile, "application/x-gzip", "jdk.tar.gz");
    }

    @GET
    @Path("/cassandra.tar.gz")
    public Response cassandraTar() {
        return handleRequest(cassandraTarFile, "application/x-gzip", "cassandra.tar.gz");
    }

    @NotNull
    private Response handleRequest(@NotNull final File resource, @NotNull final String type, @NotNull final String attachmentName) {
        final Response.ResponseBuilder builder = Response.ok(resource, type);
        builder.header("Content-Disposition", String.format("attachment; filename=\"%s\"", attachmentName));
        return builder.build();
    }

    @NotNull
    private static File verifyFileExistsAndCanRead(@NotNull final String path) {
        final File file = new File(path);
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("Unable to read specified resource: " + path);
        }
        return file;
    }

}
