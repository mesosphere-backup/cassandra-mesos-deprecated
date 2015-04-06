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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

@Path("/")
public final class ApiController {

    @NotNull
    private final JsonFactory factory;

    public ApiController(final @NotNull JsonFactory factory) {
        this.factory = factory;
    }

    /**
     * Basially a poor man's index page.
     */
    @GET
    @Produces("application/json")
    public List<ApiEndpoint> indexPage(@Context final UriInfo uriInfo) {
        final String baseUri = uriInfo.getBaseUri().toString();
        return newArrayList(
                new ApiEndpoint("GET",  baseUri + "config", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/cleanup/start", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/cleanup/abort", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/cleanup/status", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/cleanup/last", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/repair/start", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/repair/abort", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/repair/status", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/repair/last", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/rolling-restart/start", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "cluster/rolling-restart/abort", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/rolling-restart/status", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "cluster/rolling-restart/last", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "node/all", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "node/seed/all", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/stop/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/start/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/restart/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/terminate/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/replace/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/make-seed/", newArrayList("application/json")),
                new ApiEndpoint("POST", baseUri + "node/{node}/make-non-seed/", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "live-nodes", newArrayList("application/json")),
                new ApiEndpoint("GET",  baseUri + "live-nodes/text", newArrayList("text/plain")),
                new ApiEndpoint("GET",  baseUri + "live-nodes/cqlsh", newArrayList("text/x-cassandra-cqlsh")),
                new ApiEndpoint("GET",  baseUri + "live-nodes/nodetool", newArrayList("text/x-cassandra-nodetool")),
                new ApiEndpoint("GET",  baseUri + "live-nodes/stress", newArrayList("text/x-cassandra-stress")),
                new ApiEndpoint("GET",  baseUri + "qa/report/resources", newArrayList("application/json", "text/plain"))
        );
    }

    public static final class ApiEndpoint {
        @NotNull
        private final String method;
        @NotNull
        private final String url;
        @NotNull
        private final List<String> contentType;

        @JsonCreator
        public ApiEndpoint(
                @NotNull @JsonProperty("method") final String method,
                @NotNull @JsonProperty("url") final String url,
                @NotNull @JsonProperty("contentType") final List<String> contentType
        ) {
            this.method = method;
            this.url = url;
            this.contentType = contentType;
        }

        @NotNull
        public String getMethod() {
            return method;
        }

        @NotNull
        public String getUrl() {
            return url;
        }

        @NotNull
        public List<String> getContentType() {
            return contentType;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final ApiEndpoint that = (ApiEndpoint) o;

            if (!method.equals(that.method)) return false;
            if (!url.equals(that.url)) return false;
            return contentType.equals(that.contentType);

        }

        @Override
        public int hashCode() {
            int result = method.hashCode();
            result = 31 * result + url.hashCode();
            result = 31 * result + contentType.hashCode();
            return result;
        }
    }
}
