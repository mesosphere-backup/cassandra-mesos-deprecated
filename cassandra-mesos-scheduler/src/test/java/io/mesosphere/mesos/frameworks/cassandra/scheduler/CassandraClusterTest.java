package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CassandraClusterTest {

    @Test
    public void nextPossibleServerLaunchTimestamp() throws Exception {
        assertThat(CassandraCluster.nextPossibleServerLaunchTimestamp(0, 60, 60)).isEqualTo(60_000L);
        assertThat(CassandraCluster.nextPossibleServerLaunchTimestamp(9, 60, 60)).isEqualTo(60_009L);
    }

    @Test
    public void nextPossibleServerLaunchTimestamp_healthCheckTimeoutIsUsedWhenGracePeriodIsSmaller() throws Exception {
        assertThat(CassandraCluster.nextPossibleServerLaunchTimestamp(0, 15, 60)).isEqualTo(60_000L);
    }

    @Test
    public void canLaunchServerTask_1() throws Exception {
        assertThat(CassandraCluster.canLaunchServerTask(0, 10)).isFalse();
    }

    @Test
    public void canLaunchServerTask_2() throws Exception {
        assertThat(CassandraCluster.canLaunchServerTask(9, 10)).isFalse();
    }

    @Test
    public void canLaunchServerTask_3() throws Exception {
        assertThat(CassandraCluster.canLaunchServerTask(10, 10)).isTrue();
    }

    @Test
    public void canLaunchServerTask_4() throws Exception {
        assertThat(CassandraCluster.canLaunchServerTask(11, 10)).isTrue();
    }

    @Test
    public void secondsUntilNextPossibleServerLaunch_1() throws Exception {
        assertThat(CassandraCluster.secondsUntilNextPossibleServerLaunch(0, 10)).isEqualTo(0);
    }

    @Test
    public void secondsUntilNextPossibleServerLaunch_2() throws Exception {
        assertThat(CassandraCluster.secondsUntilNextPossibleServerLaunch(0, 10_000)).isEqualTo(10);
    }

    @Test
    public void secondsUntilNextPossibleServerLaunch_3() throws Exception {
        assertThat(CassandraCluster.secondsUntilNextPossibleServerLaunch(10_000, 10_000)).isEqualTo(0);
    }

    @Test
    public void secondsUntilNextPossibleServerLaunch_4() throws Exception {
        assertThat(CassandraCluster.secondsUntilNextPossibleServerLaunch(9_000, 10_000)).isEqualTo(1);
    }

    @Test
    public void secondsUntilNextPossibleServerLaunch_5() throws Exception {
        assertThat(CassandraCluster.secondsUntilNextPossibleServerLaunch(11_000, 10_000)).isEqualTo(0);
    }
}
