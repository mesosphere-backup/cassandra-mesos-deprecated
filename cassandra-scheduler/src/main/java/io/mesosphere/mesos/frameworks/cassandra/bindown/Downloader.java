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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public abstract class Downloader {
    private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);

    public abstract String getFilename();

    protected abstract String getDownloadURL();

    public void download(OutputStream target) throws IOException {
        String urlStr = getDownloadURL();
        LOGGER.info("Downloading from URL {}", urlStr);
        HttpURLConnection urlConn;

        byte[] buf = new byte[4096];
        String urlStrCurrent = urlStr;
        while (true) {
            URL url = new URL(urlStrCurrent);
            urlConn = (HttpURLConnection) url.openConnection();
            updateURLConnection(urlConn);
            urlConn.connect();
            int rc = urlConn.getResponseCode();
            if (rc == 200)
                break;
            if (rc == 302) {
                urlConn.disconnect();
                urlStrCurrent = urlConn.getHeaderField("Location");
                continue;
            }
            throw new IOException("Got response code " + rc + " from url " + urlStrCurrent);
        }
        long total = 0;
        try (InputStream is = urlConn.getInputStream()) {
            int rd;
            while ((rd = is.read(buf)) >= 0) {
                target.write(buf, 0, rd);
                total += rd;
            }
            LOGGER.info("Downloaded {} bytes from {}", total, urlStr);
            if (total == 0)
                throw new EOFException("No data from " + urlStr);
        } finally {
            urlConn.disconnect();
        }
    }

    protected void updateURLConnection(HttpURLConnection urlConn) {

    }

    public void download(File target) throws IOException {
        File tmp = new File(target.getParentFile(), target.getName() + ".tmp");
        try {
            try (FileOutputStream out = new FileOutputStream(tmp)) {
                download(out);
            }
            target.delete();
            tmp.renameTo(target);
        } catch (IOException e) {
            tmp.delete();
            throw e;
        } catch (Exception e) {
            tmp.delete();
            throw new RuntimeException(e);
        }
    }
}
