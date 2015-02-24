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
package io.mesosphere.mesos.frameworks.cassandra.jmx;

import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeCleanupJob extends AbstractKeyspacesJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCleanupJob.class);

    public NodeCleanupJob() {
    }

    @Override
    public CassandraTaskProtos.KeyspaceJobType getType() {
        return CassandraTaskProtos.KeyspaceJobType.CLEANUP;
    }

    public boolean start(JmxConnect jmxConnect) {
        if (!super.start(jmxConnect))
            return false;

        LOGGER.info("Initiated cleanup job for keyspaces {}", getKeyspaces());

        return true;
    }

    @Override
    public void startNextKeyspace() {
        throw new UnsupportedOperationException();
    }

    public void cleanupBlocking() {
        while (true) {
            String keyspace = nextKeyspace();
            if (keyspace == null)
                break;

            int statusOrdinal = 0;
            try {
                statusOrdinal = jmxConnect.getStorageServiceProxy().forceKeyspaceCleanup(keyspace);
            } catch (Exception e) {
                LOGGER.error("Cleanup for keyspace " + keyspace + " failed", e);
            }
            CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.values()[statusOrdinal];
            keyspaceFinished(status.name(), keyspace);
            LOGGER.info("Cleanup for keyspace {} finished with status {}", keyspace, status);
        }
        finished();
    }

    protected void finished() {
        super.finished();
    }

}
