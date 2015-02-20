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
import java.io.IOException;

public class JreLoaderTest {
    @Test
    public void testForOsNameAndArch() {
        String osName = "Mac OS X"; //System.getProperty("os.name");
        String osArch = "x86_64"; //System.getProperty("os.arch");

        JreLoader jreLoader = JreLoader.forOsNameAndArch(JreLoader.TYPE_JRE, "7u76", "b13", osName, osArch);
        Assert.assertEquals(jreLoader.getOs(), "macosx");
        Assert.assertEquals(jreLoader.getArch(), "x64");
        Assert.assertEquals(jreLoader.getType(), JreLoader.TYPE_JRE);
        Assert.assertEquals(jreLoader.getVersion(), "7u76");
        Assert.assertEquals(jreLoader.getBuild(), "b13");

        osName = "Foo Bar Linux Distro";
        jreLoader = JreLoader.forOsNameAndArch(JreLoader.TYPE_JRE, "7u76", "b13", osName, osArch);
        Assert.assertEquals(jreLoader.getOs(), "linux");
        Assert.assertEquals(jreLoader.getArch(), "x64");
    }

    @Test
    public void testDownload() throws IOException {
        JreLoader jreLoader = JreLoader.forCurrendOS(JreLoader.TYPE_JRE, "7u76", "b13");
        Assert.assertEquals(jreLoader.getType(), JreLoader.TYPE_JRE);
        Assert.assertEquals(jreLoader.getVersion(), "7u76");
        Assert.assertEquals(jreLoader.getBuild(), "b13");

        File file = File.createTempFile("io.mesosphere.mesos.frameworks.cassandra.bindown.JreLoaderTest." + jreLoader.getFilename(), "");
        file.deleteOnExit();

        jreLoader.download(file);

        Assert.assertTrue(file.exists());
        Assert.assertFalse(new File(file.getParent(), file.getName() + ".tmp").exists());
        Assert.assertTrue(file.length() > 32 * 1024 * 1024);
    }
}