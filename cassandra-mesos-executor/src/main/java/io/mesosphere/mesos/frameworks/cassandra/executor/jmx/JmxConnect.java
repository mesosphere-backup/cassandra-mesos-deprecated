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
package io.mesosphere.mesos.frameworks.cassandra.executor.jmx;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.*;

import java.io.Closeable;
import java.lang.management.RuntimeMXBean;
import java.util.*;

/**
 * JMX interface class to Cassandra node.
 * The JMX via RMI connection is lazily created when the first call requires it.
 *
 * @author Robert Stupp
 */
public interface JmxConnect extends Closeable {

    RuntimeMXBean getRuntimeProxy();

    StorageServiceMBean getStorageServiceProxy();

    EndpointSnitchInfoMBean getEndpointSnitchInfoProxy();

    List<String> getColumnFamilyNames(String keyspace);
}
