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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraFrameworkConfiguration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class CassandraConfigRoleTest {
    @Test(expected = IllegalArgumentException.class)
    public void testMemoryParametersNone() {
        CassandraFrameworkProtos.CassandraConfigRole.Builder builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(0)
                .setDiskMb(1)
                .setCpuCores(1));
        PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
    }

    @Test()
    public void testMemoryParameters() {
        CassandraFrameworkProtos.CassandraConfigRole.Builder builder;
        CassandraFrameworkProtos.CassandraConfigRole configRole;

        builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(8192)
                .setDiskMb(1)
                .setCpuCores(1));
        configRole = PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
        assertThat(configRole.getResources().getMemMb()).isEqualTo(8192);
        assertThat(configRole.getMemAssumeOffHeapMb()).isEqualTo(4096);
        assertThat(configRole.getMemJavaHeapMb()).isEqualTo(4096);

        builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(0)
                .setDiskMb(1)
                .setCpuCores(1))
            .setMemJavaHeapMb(4096);
        configRole = PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
        assertThat(configRole.getResources().getMemMb()).isEqualTo(8192);
        assertThat(configRole.getMemAssumeOffHeapMb()).isEqualTo(4096);
        assertThat(configRole.getMemJavaHeapMb()).isEqualTo(4096);

        builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(0)
                .setDiskMb(1)
                .setCpuCores(1))
            .setMemAssumeOffHeapMb(4096);
        configRole = PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
        assertThat(configRole.getResources().getMemMb()).isEqualTo(8192);
        assertThat(configRole.getMemAssumeOffHeapMb()).isEqualTo(4096);
        assertThat(configRole.getMemJavaHeapMb()).isEqualTo(4096);

        builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(0)
                .setDiskMb(1)
                .setCpuCores(1))
            .setMemJavaHeapMb(4096)
            .setMemAssumeOffHeapMb(4096);
        configRole = PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
        assertThat(configRole.getResources().getMemMb()).isEqualTo(8192);
        assertThat(configRole.getMemAssumeOffHeapMb()).isEqualTo(4096);
        assertThat(configRole.getMemJavaHeapMb()).isEqualTo(4096);

        builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                .setMemMb(10000)
                .setDiskMb(1)
                .setCpuCores(1))
            .setMemJavaHeapMb(4096)
            .setMemAssumeOffHeapMb(4096);
        configRole = PersistedCassandraFrameworkConfiguration.fillConfigRoleGaps(builder).build();
        assertThat(configRole.getResources().getMemMb()).isEqualTo(10000);
        assertThat(configRole.getMemAssumeOffHeapMb()).isEqualTo(4096);
        assertThat(configRole.getMemJavaHeapMb()).isEqualTo(4096);
    }
}
