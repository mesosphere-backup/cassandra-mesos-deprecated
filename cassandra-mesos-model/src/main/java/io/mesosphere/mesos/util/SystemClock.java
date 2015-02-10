package io.mesosphere.mesos.util;

import org.jetbrains.annotations.NotNull;
import org.joda.time.Instant;

public final class SystemClock implements Clock {
    @NotNull
    @Override
    public Instant now() {
        return Instant.now();
    }
}
