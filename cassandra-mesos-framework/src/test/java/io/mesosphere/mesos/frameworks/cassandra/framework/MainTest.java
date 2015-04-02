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
package io.mesosphere.mesos.frameworks.cassandra.framework;

import org.junit.Test;

import java.util.regex.Matcher;

import static io.mesosphere.mesos.frameworks.cassandra.framework.Main.*;
import static org.assertj.core.api.Assertions.assertThat;

public class MainTest {

    @Test
    public void testZkDirectoryDepth1() throws Exception {
        final Matcher m = validateZkUrl("zk://host:2181/cassandra-mesos");
        assertThat(m.group(1)).isEqualTo("host:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos");
    }

    @Test
    public void testZkDirectoryDepth2() throws Exception {
        final Matcher m = validateZkUrl("zk://host:2181/cassandra-mesos/ring1");
        assertThat(m.group(1)).isEqualTo("host:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos/ring1");
    }

    @Test
    public void testZkDirectoryDepth3() throws Exception {
        final Matcher m = validateZkUrl("zk://host:2181/cassandra-mesos/a/b/c");
        assertThat(m.group(1)).isEqualTo("host:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos/a/b/c");
    }
    
    @Test
    public void testZkDirectoryDepth4() throws Exception {
        final Matcher m = validateZkUrl("zk://host:2181/cassandra-mesos/a/b/c/d");
        assertThat(m.group(1)).isEqualTo("host:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos/a/b/c/d");
    }

    @Test
    public void test2Hosts() throws Exception {
        final Matcher m = validateZkUrl("zk://host1:2181,host2:2181/cassandra-mesos/a/b/c/d");
        assertThat(m.group(1)).isEqualTo("host1:2181,host2:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos/a/b/c/d");
    }

    @Test
    public void test3Hosts() throws Exception {
        final Matcher m = validateZkUrl("zk://host1:2181,host2:2181,host3:2181/cassandra-mesos/a/b/c/d");
        assertThat(m.group(1)).isEqualTo("host1:2181,host2:2181,host3:2181");
        assertThat(m.group(2)).isEqualTo("/cassandra-mesos/a/b/c/d");
    }

    @Test(expected = SystemExitException.class)
    public void testMissingScheme() throws Exception {
        validateZkUrl("host1:2181/cassandra-mesos");
    }

    @Test(expected = SystemExitException.class)
    public void testMissingDir() throws Exception {
        validateZkUrl("zk://host1:2181/");
    }

    @Test(expected = SystemExitException.class)
    public void testTrailingSlash() throws Exception {
        validateZkUrl("zk://host1:2181/cassandra-mesos/");
    }

}
