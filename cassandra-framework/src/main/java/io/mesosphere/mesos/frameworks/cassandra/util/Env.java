package io.mesosphere.mesos.frameworks.cassandra.util;

import com.google.common.base.Optional;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Env {

    @Nullable
    public static String get(@NotNull final String key) {
        return System.getenv(key);
    }

    @NotNull
    public static Optional<String> option(@NotNull final String key) {
        return Optional.fromNullable(get(key));
    }

    @NotNull
    public static String getOrElse(@NotNull final String key, @NotNull final String defaultValue) {
        final String envValue = get(key);
        if (envValue != null) {
            return envValue;
        } else {
            return defaultValue;
        }
    }

}
