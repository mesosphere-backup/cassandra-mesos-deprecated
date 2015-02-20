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
package io.mesosphere.mesos.frameworks.cassandra.bindown;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;

public class ApacheCloserTest {
    @Test
    public void testResolve() throws Exception {
        ApacheCloser apacheCloser = new ApacheCloser("cassandra", "2.1.2", "apache-cassandra-2.1.2-bin.tar.gz");

        Assert.assertEquals(apacheCloser.getCloserCgiURL(), "https://www.apache.org/dyn/closer.cgi?path=/cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz&asjson=1");

        apacheCloser.setCloserJSON("{\n" +
                "    \"backup\": [\n" +
                "        \"http://www.eu.apache.org/dist/\",\n" +
                "        \"http://www.us.apache.org/dist/\"\n" +
                "    ],\n" +
                "    \"ftp\": [\n" +
                "        \"ftp://ftp.halifax.rwth-aachen.de/apache/\",\n" +
                "        \"ftp://ftp.fu-berlin.de/unix/www/apache/\",\n" +
                "        \"ftp://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/\"\n" +
                "    ],\n" +
                "    \"http\": [\n" +
                "        \"http://ftp.halifax.rwth-aachen.de/apache/\",\n" +
                "        \"http://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/\"\n" +
                "    ],\n" +
                "    \"location\": \"/dyn/closer.cgi\",\n" +
                "    \"path_info\": \"cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz\",\n" +
                "    \"preferred\": \"http://ftp.halifax.rwth-aachen.de/apache/\"\n" +
                '}');

        Assert.assertEquals(apacheCloser.getPreferredDownloadURL(), "http://ftp.halifax.rwth-aachen.de/apache/cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz");

        Assert.assertEquals(apacheCloser.getLocation(), "/dyn/closer.cgi");
        Assert.assertEquals(apacheCloser.getPathInfo(), "cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz");
        Assert.assertEquals(apacheCloser.getPreferred(), "http://ftp.halifax.rwth-aachen.de/apache/");
        Assert.assertEquals(apacheCloser.getBackup().size(), 2);
        Assert.assertEquals(apacheCloser.getMirrors().size(), 5);
        Assert.assertTrue(apacheCloser.getBackup().contains("http://www.us.apache.org/dist/"));
        Assert.assertTrue(apacheCloser.getMirrors().contains("ftp://ftp.fu-berlin.de/unix/www/apache/"));
    }

    @Test
    public void testRealResolve() throws Exception {
        ApacheCloser apacheCloser = new ApacheCloser("cassandra", "2.1.2", "apache-cassandra-2.1.2-bin.tar.gz");

        apacheCloser.fetchCloserCGI();

        Assert.assertEquals(apacheCloser.getLocation(), "/dyn/closer.cgi");
        Assert.assertEquals(apacheCloser.getPathInfo(), "cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz");
        Assert.assertTrue(apacheCloser.getPreferredDownloadURL().endsWith("apache-cassandra-2.1.2-bin.tar.gz"));
    }

    @Test
    public void testRealCassandra() throws Exception {
        ApacheCassandraDownloader cstar = new ApacheCassandraDownloader("2.1.2");

        cstar.fetchCloserCGI();

        Assert.assertEquals(cstar.getLocation(), "/dyn/closer.cgi");
        Assert.assertEquals(cstar.getPathInfo(), "cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz");
        Assert.assertTrue(cstar.getPreferredDownloadURL().endsWith("apache-cassandra-2.1.2-bin.tar.gz"));

        File file = File.createTempFile("io.mesosphere.mesos.frameworks.cassandra.bindown.ApacheCassandraDownloader." + cstar.getFilename(), "");
        file.deleteOnExit();

        cstar.download(file);

        Assert.assertTrue(file.exists());
        Assert.assertFalse(new File(file.getParent(), file.getName() + ".tmp").exists());
        Assert.assertTrue(file.length() > 10 * 1024 * 1024);
    }
}
