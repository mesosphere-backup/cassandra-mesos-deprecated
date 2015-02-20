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

    public FileResourceController() {
        cassandraExecutorFile = verifyFileExistsAndCanRead(Env.option("EXECUTOR_FILE_PATH").or(workingDir("/cassandra-executor.jar")));
    }

    @GET
    @Path("/cassandra-executor.jar")
    public Response cassandraExecutorJar() {
        return handleRequest(cassandraExecutorFile, "application/java-archive", "cassandra-executor.jar");
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
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("Unable to read specified resource: " + path);
        }
        return file;
    }

}
