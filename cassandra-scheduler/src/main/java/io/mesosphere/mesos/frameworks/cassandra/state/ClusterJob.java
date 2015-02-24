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

import com.google.common.collect.Maps;
import org.apache.mesos.Protos;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for all cluster-wide operations.
 */
public abstract class ClusterJob {
    protected final CassandraCluster cassandraCluster;

    private final ConcurrentMap<Protos.ExecutorID, ExecutorMetadata> remainingNodes;

    public ClusterJob(CassandraCluster cassandraCluster) {
        this.cassandraCluster = cassandraCluster;

        remainingNodes = Maps.newConcurrentMap();
        remainingNodes.putAll(cassandraCluster.executorMetadataMap);
    }

    public boolean hasRemainingNodes() {
        return !remainingNodes.isEmpty();
    }

    public Collection<ExecutorMetadata> allRemainingNodes() {
        return remainingNodes.values();
    }

    protected ExecutorMetadata remainingNodeForExecutor(Protos.ExecutorID executorID) {
        return remainingNodes.remove(executorID);
    }

    // TODO move common code for other cluser-wide jobs from RepairJob here
}
