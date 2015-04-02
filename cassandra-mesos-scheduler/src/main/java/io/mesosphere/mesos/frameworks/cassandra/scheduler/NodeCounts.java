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

public final class NodeCounts {
    private final int nodeCount;
    private final int seedCount;

    public NodeCounts(int nodeCount, int seedCount) {
        this.nodeCount = nodeCount;
        this.seedCount = seedCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getSeedCount() {
        return seedCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeCounts that = (NodeCounts) o;

        return nodeCount == that.nodeCount && seedCount == that.seedCount;

    }

    @Override
    public int hashCode() {
        int result = nodeCount;
        result = 31 * result + seedCount;
        return result;
    }
}
