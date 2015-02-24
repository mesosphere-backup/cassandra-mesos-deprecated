/**
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
package io.mesosphere.mesos.frameworks.cassandra.state;

import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;

public final class RepairJob extends ClusterJob<CassandraTaskProtos.KeyspaceJobStatus> {

    private static final long REPAIR_STATUS_INVERVAL = 10000L;

    RepairJob(CassandraCluster cassandraCluster) {
        super(cassandraCluster, null);
    }

    @Override
    protected boolean statusIsFinished(CassandraTaskProtos.KeyspaceJobStatus status) {
        return !status.getRunning();
    }

    @Override
    protected boolean checkNodeStatus(CassandraTaskProtos.CassandraNodeHealthCheckDetails hc) {
        return super.checkNodeStatus(hc) && "NORMAL".equals(hc.getInfo().getOperationMode());
    }

    @Override
    protected long statusInterval() {
        return REPAIR_STATUS_INVERVAL;
    }
}
