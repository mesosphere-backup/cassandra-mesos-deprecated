package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;

public final class Functions {

    private Functions() {}

    @NotNull
    public static Function<String, Protos.CommandInfo.URI> extract() {
        return CommandInfoUri.INSTANCE_EXTRACT;
    }

    @NotNull
    public static Function<String, Protos.CommandInfo.URI> doNotExtract() {
        return CommandInfoUri.INSTANCE;
    }

    private static final class CommandInfoUri implements Function<String, Protos.CommandInfo.URI> {
        private static final CommandInfoUri INSTANCE = new CommandInfoUri();

        private static final CommandInfoUri INSTANCE_EXTRACT = new CommandInfoUri(true);

        private final boolean extract;

        public CommandInfoUri() {
            this(false);
        }

        public CommandInfoUri(final boolean extract) {
            this.extract = extract;
        }
        @Override
        @NotNull
        public Protos.CommandInfo.URI apply(final String input) {
            final boolean shouldExtract = extract;
            return ProtoUtils.commandUri(input, shouldExtract);
        }
    }

}
