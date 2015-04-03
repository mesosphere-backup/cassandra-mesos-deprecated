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

import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Env;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.File;

import static io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Env.workingDir;

@Path("/")
public final class FileResourceController {
    @NotNull
    private final File cassandraExecutorFile;
    @NotNull
    private final File jreTarFile;
    @NotNull
    private final File cassandraTarFile;

    public FileResourceController(String cassandraVersion) {
        File f;

        String javaVersion = Env.option("JAVA_VERSION").or("7u76");
        String osName = Env.osFromSystemProperty();
        String providedJreTar = Env.option("JRE_FILE_PATH").or(workingDir("/jre-" + javaVersion + '-' + osName + "-x64.tar.gz"));
        //if (providedJreTar != null)
        f = new File(providedJreTar);
        //if (f != null && !f.canRead())
        //    f = null;
        verifyFileExistsAndCanRead(f);
        jreTarFile = f;

        String providedCassandraTar = Env.option("CASSANDRA_FILE_PATH").or(workingDir("/apache-cassandra-" + cassandraVersion + "-bin.tar.gz"));
        //f = null;
        //if (providedCassandraTar != null)
        f = new File(providedCassandraTar);
        //if (f != null && !f.canRead())
        //    f = null;
        verifyFileExistsAndCanRead(f);
        cassandraTarFile = f;


        cassandraExecutorFile = verifyFileExistsAndCanRead(Env.option("EXECUTOR_FILE_PATH").or(workingDir("/cassandra-mesos-executor.jar")));
    }

    @GET
    @Path("/cassandra-executor.jar")
    public Response cassandraExecutorJar() {
        return handleRequest(cassandraExecutorFile, "application/java-archive", "cassandra-executor.jar");
    }

    @GET
    @Path("/jre-{version}-{osname}.tar.gz")
    public Response jreTar(@PathParam("version") String version, @PathParam("osname") String osname) {
        // version is currently unused
        // But we might need that parameter not too far away in the future since C* 3.x probably requires Java 8,
        // while older versions still require Java 7.
        return handleRequest(jreTarFile, "application/x-gzip", "jre.tar.gz");
    }

    @GET
    @Path("/apache-cassandra-{version}-bin.tar.gz")
    public Response cassandraTar(@PathParam("version") String version) {
        return handleRequest(cassandraTarFile, "application/x-gzip", "cassandra.tar.gz");
    }

    @NotNull
    private static Response handleRequest(@NotNull final File resource, @NotNull final String type, @NotNull final String attachmentName) {
        final Response.ResponseBuilder builder = Response.ok(resource, type);
        builder.header("Content-Disposition", String.format("attachment; filename=\"%s\"", attachmentName));
        return builder.build();
    }

    @NotNull
    private static File verifyFileExistsAndCanRead(@NotNull final String path) {
        final File file = new File(path);
        return verifyFileExistsAndCanRead(file);
    }

    private static File verifyFileExistsAndCanRead(File file) {
        if (!file.isFile() || !file.canRead()) {
            throw new IllegalArgumentException("Unable to read specified resource: " + file);
        }
        return file;
    }

}
