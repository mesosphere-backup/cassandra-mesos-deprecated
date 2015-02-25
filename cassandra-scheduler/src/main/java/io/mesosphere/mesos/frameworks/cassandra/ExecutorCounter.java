package io.mesosphere.mesos.frameworks.cassandra;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

import static io.mesosphere.mesos.frameworks.cassandra.util.Futures.await;

final class ExecutorCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorCounter.class);

    public static final String VARIABLE_NAME = "executor-counter";
    @NotNull
    private final State state;
    private final long defaultValue;

    @NotNull
    private Variable var;

    public ExecutorCounter(final @NotNull State state, final long defaultValue) {
        this.state = state;
        this.defaultValue = defaultValue;

        this.var = await(state.fetch(VARIABLE_NAME));
    }

    public long get() {
        LOGGER.debug("> get()");
        final byte[] value = var.value();
        final long retVal;
        if (value.length > 0) {
            retVal = new BigInteger(value).longValue();
        } else {
            retVal = defaultValue;
        }
        LOGGER.trace("< get() = {}", retVal);
        return retVal;
    }

    public long getAndIncrement() {
        LOGGER.debug("> getAndIncrement()");
        final long l = get();
        final long increment = l + 1;
        var = setValue(BigInteger.valueOf(increment));
        LOGGER.trace("< getAndIncrement() = {}", l);
        return l;
    }

    private Variable setValue(@NotNull final BigInteger newValue) {
        return await(state.store(var.mutate(newValue.toByteArray())));
    }
}
