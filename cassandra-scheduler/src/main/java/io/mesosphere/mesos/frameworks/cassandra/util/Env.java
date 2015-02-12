package io.mesosphere.mesos.frameworks.cassandra.util;

import com.google.common.base.Optional;
import org.jetbrains.annotations.NotNull;

public final class Env {

    @NotNull
    public static String get(@NotNull final String key) {
        final Optional<String> opt = option(key);
        if (opt.isPresent()) {
            return opt.get();
        } else {
            throw new IllegalStateException(String.format("Environment variable %s is not defined", key));
        }
    }

    @NotNull
    public static Optional<String> option(@NotNull final String key) {
        return Optional.fromNullable(System.getenv(key));
    }

    @NotNull
    public static String workingDir(final String defaultFileName) {
        return System.getProperty("user.dir") + defaultFileName;
    }

}
