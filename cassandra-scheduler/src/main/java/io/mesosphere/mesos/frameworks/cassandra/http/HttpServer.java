package io.mesosphere.mesos.frameworks.cassandra.http;

import com.google.common.collect.Lists;
import io.mesosphere.mesos.util.Tuple2;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static io.mesosphere.mesos.util.Tuple2.tuple2;

public final class HttpServer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);

    @NotNull
    private final Undertow server;
    private final int bindPort;
    @NotNull
    private final String bindInterface;
    @NotNull
    private final InetAddress localHost;

    public HttpServer(final int bindPort, @NotNull final String bindInterface, @NotNull final Undertow server) {
        this.bindPort = bindPort;
        this.bindInterface = bindInterface;
        this.server = server;
        try {
            localHost = InetAddress.getLocalHost();
        } catch (final UnknownHostException e) {
            LOGGER.error("Unable to resolve localhost interface", e);
            throw new RuntimeException(e);
        }
    }

    public void start() {
        server.start();
    }

    @NotNull
    public String getBoundAddress() {
        final String address;
        if ("0.0.0.0".equals(bindInterface) || "::".equals(bindInterface)) {
            address = formatInetAddress(localHost);
        } else {
            address = bindInterface;
        }
        return String.format("%s:%d", address, bindPort);

    }

    @NotNull
    public String getUrlForResource(@NotNull final String resource) {
        return String.format("http://%s/%s", getBoundAddress(), resource).replaceAll("(?<!:)/+", "/");
    }

    @Override
    public void close() throws IOException {
        server.stop();
    }

    @NotNull
    private static String formatInetAddress(@NotNull final InetAddress inetAddress) {
        if (inetAddress instanceof Inet4Address) {
            final Inet4Address address = (Inet4Address) inetAddress;
            return address.getHostAddress();
        } else if (inetAddress instanceof Inet6Address) {
            final Inet6Address address = (Inet6Address) inetAddress;
            return String.format("[%s]", address.getHostAddress());
        } else {
            throw new IllegalArgumentException("InetAddress type: " + inetAddress.getClass().getName() + " is not supported");
        }
    }

    @NotNull
    public static HttpServerBuilder newBuilder() {
        return new HttpServerBuilder();
    }

    public static final class HttpServerBuilder {
        private int bindPort;
        private String bindInterface;
        private final List<Tuple2<String, HttpHandler>> httpHandlers;

        public HttpServerBuilder() {
            httpHandlers = Lists.newArrayList();
        }

        @NotNull
        public HttpServerBuilder withBindPort(final int bindPort) {
            this.bindPort = bindPort;
            return this;
        }

        @NotNull
        public HttpServerBuilder withBindInterface(@NotNull final String bindInterface) {
            this.bindInterface = bindInterface;
            return this;
        }

        @NotNull
        public HttpServerBuilder withPathHandler(@NotNull final String path, @NotNull final HttpHandler handler) {
            httpHandlers.add(tuple2(path, handler));
            return this;
        }

        @NotNull
        public HttpServer build() {
            final Undertow.Builder builder = Undertow.builder()
                    .addHttpListener(bindPort, bindInterface);
            final PathHandler handler = new PathHandler();
            for (final Tuple2<String, HttpHandler> pathHandler : httpHandlers) {
                handler.addPrefixPath(pathHandler._1, pathHandler._2);
            }
            builder.setHandler(handler);
            final Undertow undertow = builder.build();

            return new HttpServer(bindPort, bindInterface, undertow);
        }

    }
}
