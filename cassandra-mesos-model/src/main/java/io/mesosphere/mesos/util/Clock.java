package io.mesosphere.mesos.util;

import org.jetbrains.annotations.NotNull;
import org.joda.time.Instant;

public interface Clock {
    @NotNull
    public Instant now();

}
