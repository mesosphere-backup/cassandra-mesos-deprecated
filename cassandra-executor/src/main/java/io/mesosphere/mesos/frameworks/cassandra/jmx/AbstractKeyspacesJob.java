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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AbstractKeyspacesJob implements Closeable {

    protected final long startTimestamp = System.currentTimeMillis();

    protected JmxConnect jmxConnect;
    protected List<String> keyspaces;

    protected AbstractKeyspacesJob() {

    }

    protected boolean start(JmxConnect jmxConnect) {
        this.jmxConnect = jmxConnect;

        if (!"NORMAL".equals(jmxConnect.getStorageServiceProxy().getOperationMode()))
            return false;

        keyspaces = new CopyOnWriteArrayList<>(jmxConnect.getStorageServiceProxy().getKeyspaces());
        keyspaces.remove("system");
//        keyspaces.remove("system_auth");
//        keyspaces.remove("system_traces");

        return true;
    }

    @Override
    public void close() {
        try {
            jmxConnect.close();
        } catch (IOException e) {
            // ignore
        }
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }
}