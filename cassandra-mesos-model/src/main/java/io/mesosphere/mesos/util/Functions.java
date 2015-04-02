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
package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

public final class Functions {

    private Functions() {}

    public static final Comparator<Long> naturalLongComparator = new Comparator<Long>() {
        @Override
        public int compare(final Long o1, final Long o2) {
            return Long.compare(o1, o2);
        }
    };

    @NotNull
    public static <A> Optional<A> headOption(@NotNull final Iterable<A> iterable) {
        final Iterator<A> iter = iterable.iterator();
        if (iter.hasNext()) {
            return Optional.fromNullable(iter.next());
        } else {
            return Optional.absent();
        }
    }

    @NotNull
    @SafeVarargs
    public static <K, V> Map<K, V> unmodifiableHashMap(@NotNull final Tuple2<K, V>... tuples) {
        final Map<K, V> map = Maps.newHashMap();
        for (final Tuple2<K, V> tuple : tuples) {
            map.put(tuple._1, tuple._2);
        }
        return Collections.unmodifiableMap(map);
    }

    @NotNull
    public static <A> List<A> append(@NotNull final List<A> list, @NotNull final A item) {
        final List<A> as = newArrayList();
        as.addAll(list);
        as.add(item);
        return as;
    }

    @NotNull
    public static Function<String, Protos.CommandInfo.URI> extract() {
        return CommandInfoUri.INSTANCE_EXTRACT;
    }

    @NotNull
    public static Function<String, Protos.CommandInfo.URI> doNotExtract() {
        return CommandInfoUri.INSTANCE;
    }

    public static abstract class ContinuingTransform<A> implements Function<A, A> {
        public abstract A apply(final A input);
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
