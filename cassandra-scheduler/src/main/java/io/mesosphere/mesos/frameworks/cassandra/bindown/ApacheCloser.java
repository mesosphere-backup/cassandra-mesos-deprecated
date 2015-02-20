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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Tool to download a binary artifact from Apache using ASF's {@code closer.cgi} to find a mirror.
 * <p>
 * For example the request {@code https://www.apache.org/dyn/closer.cgi?path=/cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz&asjson=1}
 * returns a JSON document like this:
 * <code><pre>{
 *     "backup": [
 *         "http://www.eu.apache.org/dist/",
 *         "http://www.us.apache.org/dist/"
 *     ],
 *     "ftp": [
 *         "ftp://ftp.halifax.rwth-aachen.de/apache/",
 *         "ftp://ftp.fu-berlin.de/unix/www/apache/",
 *         "ftp://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/"
 *     ],
 *     "http": [
 *         "http://ftp.halifax.rwth-aachen.de/apache/",
 *         "http://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/"
 *     ],
 *     "location": "/dyn/closer.cgi",
 *     "path_info": "cassandra/2.1.2/apache-cassandra-2.1.2-bin.tar.gz",
 *     "preferred": "http://ftp.halifax.rwth-aachen.de/apache/"
 * }</pre></code>
 * </p>
 * <p>
 * Basically we only need the fields {@code preferred} and {@code path_info} from the returned document unless
 * we implement some error-try-loop if fetching from the preferred fails.
 * </p>
 */
public class ApacheCloser extends Downloader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheCloser.class);

    static final String prefix = "https://www.apache.org/dyn/closer.cgi?path=";
    static final String suffix = "&asjson=1";

    //

    private final String path;
    private final String file;
    private String closerJSON;

    private final List<String> backup = new ArrayList<>();
    private final List<String> mirrors = new ArrayList<>();
    private String location;
    private String pathInfo;
    private String preferred;

    public ApacheCloser(String path, String file) {
        this.path = path;
        this.file = file;
    }

    public ApacheCloser(String project, String version, String file) {
        this('/' + project + '/' + version + '/' + file, file);
    }

    @Override
    public String getFilename() {
        return file;
    }

    public String getCloserCgiURL() {
        return prefix + path + suffix;
    }

    public String getCloserJSON() {
        return closerJSON;
    }

    // exposed for testing only
    public void setCloserJSON(String closerJSON) throws IOException {
        this.closerJSON = closerJSON;

        mirrors.clear();
        backup.clear();
        location = null;
        preferred = null;
        pathInfo = null;

        JsonFactory jsonFactory = new JsonFactory();
        try (JsonParser jsonParser = jsonFactory.createParser(closerJSON)) {
            String currentParent = null;
            String fieldName = null;
            for (JsonToken token = jsonParser.nextToken(); jsonParser.hasCurrentToken(); token = jsonParser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        fieldName = jsonParser.getCurrentName();
                        break;
                    case START_ARRAY:
                        currentParent = fieldName;
                        break;
                    case END_ARRAY:
                        currentParent = null;
                        break;
                    case VALUE_STRING:
                        if (currentParent == null) {
                            switch (fieldName) {
                                case "location":
                                    location = jsonParser.getValueAsString();
                                    break;
                                case "path_info":
                                    pathInfo = jsonParser.getValueAsString();
                                    break;
                                case "preferred":
                                    preferred = jsonParser.getValueAsString();
                                    break;
                            }
                        } else
                            switch (currentParent) {
                                case "http":
                                case "ftp":
                                    mirrors.add(jsonParser.getValueAsString());
                                    break;
                                case "backup":
                                    backup.add(jsonParser.getValueAsString());
                                    break;
                            }
                        break;
                }
            }
        }
    }

    public List<String> getBackup() {
        return backup;
    }

    public String getLocation() {
        return location;
    }

    public List<String> getMirrors() {
        return mirrors;
    }

    public String getPathInfo() {
        return pathInfo;
    }

    public String getPreferred() {
        return preferred;
    }

    public String getPreferredDownloadURL() {
        if (preferred == null || pathInfo == null)
            try {
                fetchCloserCGI();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        if (preferred == null || pathInfo == null)
            throw new IllegalStateException("no information - call fetchCloserCGI() first");
        return preferred + pathInfo;
    }

    public void fetchCloserCGI() throws IOException {
        String urlStr = getCloserCgiURL();
        HttpURLConnection urlConn;

        byte[] buf = new byte[4096];
        String urlStrCurrent = urlStr;
        while (true) {
            URL url = new URL(urlStrCurrent);
            urlConn = (HttpURLConnection) url.openConnection();
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
        StringWriter sw = new StringWriter();
        long total = 0;
        try (InputStream is = urlConn.getInputStream()) {
            int rd;
            while ((rd = is.read(buf)) >= 0) {
                // we don't care about encoding here - it's all US-ASCII
                sw.write(new String(buf, 0, rd));
                total += rd;
            }
            LOGGER.info("Downloaded {} bytes from {}", total, urlStr);
            if (total == 0)
                throw new EOFException("No data from " + urlStr);
        } finally {
            urlConn.disconnect();
        }

        setCloserJSON(sw.toString());
    }

    @Override
    protected String getDownloadURL() {
        return getPreferredDownloadURL();
    }
}
