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
package io.mesosphere.mesos.frameworks.cassandra.files;

import io.mesosphere.mesos.frameworks.cassandra.bindown.JreLoader;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.File;

@Path("/")
public class JreFileResourceController {
    static final String javaVersion = Env.option("JAVA_VERSION").or("7u76");
    static final String javaBuild = Env.option("JAVA_VERSION_BUILD").or("b13");
    private static final String providedTar = Env.option("JDK_FILE_PATH").orNull();
    private static final File provided;

    static {
        File f = null;
        if (providedTar != null)
            f = new File(providedTar);
        if (f != null && !f.canRead())
            f = null;
        provided = f;
    }


    @GET
    @Path("/jre-{osname}.tar.gz")
    public Response jdkTar(@PathParam("osname") String osname) {
        JreLoader jreLoader = new JreLoader(JreLoader.TYPE_JRE, javaVersion, javaBuild, osname, "x64", "tar.gz");
        return CachedFileResource.serveCached(jreLoader, provided);
    }

}
