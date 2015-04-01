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
