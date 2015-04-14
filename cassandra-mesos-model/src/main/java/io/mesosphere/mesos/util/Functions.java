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
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.Tuple2.tuple2;

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

    /**
     * {@code zip}s two lists together and creates a resulting list of tuples representing the value
     * at the corresponding index from each list.
     *
     * @param a A list of NonNull {@code A}s
     * @param b A list of NonNull {@code B}s
     * @return A list of {@link Tuple2} that represent the values at the corresponding index from each list.
     * The size of the returned list will be equal to {@code Math.min(a.size(), b.size())} since we don't
     * allow null values.
     */
    @NotNull
    public static <A, B> List<Tuple2<A, B>> zip(@NotNull final List<A> a, @NotNull final List<B> b) {
        final int length = Math.min(a.size(), b.size());
        final List<Tuple2<A, B>> list = newArrayList();
        for (int i = 0; i < length; i++) {
            list.add(tuple2(a.get(i), b.get(i)));
        }
        return list;
    }

    @NotNull
    public static <A> List<A> listFullOf(final int capacity, @NotNull final Supplier<A> sup) {
        final A value = sup.get();
        final List<A> list = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            list.add(value);
        }
        return list;
    }

    public static abstract class ContinuingTransform<A> implements Function<A, A> {
        public abstract A apply(final A input);
    }

}
