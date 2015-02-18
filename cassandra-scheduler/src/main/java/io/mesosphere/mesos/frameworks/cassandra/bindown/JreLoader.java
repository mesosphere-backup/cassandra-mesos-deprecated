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

import java.net.HttpURLConnection;

public class JreLoader extends Downloader {

    static final String cookie = "oraclelicense=accept-securebackup-cookie";
    static final String prefix = "http://download.oracle.com/otn-pub/java/jdk/";

    /**
     * Server-JRE is not available for OSX.
     */
    public static final String TYPE_SERVER_JRE = "server-jre";
    public static final String TYPE_JRE = "jre";
    public static final String TYPE_JDK = "jdk";

    // can be jre, jdk or server-jre (second for Linux)
    private final String type;
    private final String version;
    private final String build;
    private final String os;
    private final String arch;
    private final String extension;

    public JreLoader(String type, String version, String build, String os, String arch, String extension) {
        this.type = type;
        this.version = version;
        this.build = build;
        this.os = os;
        this.arch = arch;
        this.extension = extension;
    }

    /**
     * Construct a loader for Mac OS X.
     *
     * @param type    see one of the {@code TYPE_} constants
     * @param version e.g. {@code 7u76}
     * @param build   e.g. {@code b13}
     */
    public static JreLoader forMacOSX(String type, String version, String build) {
        return new JreLoader(type, version, build, "macosx", "x64", "tar.gz");
    }

    /**
     * Construct a loader for Linux.
     *
     * @param type    see one of the {@code TYPE_} constants
     * @param version e.g. {@code 7u76}
     * @param build   e.g. {@code b13}
     */
    public static JreLoader forLinuxX64(String type, String version, String build) {
        return new JreLoader(type, version, build, "linux", "x64", "tar.gz");
    }

    /**
     * Construct a loader for the 'current' operating system.
     *
     * @param type    see one of the {@code TYPE_} constants
     * @param version e.g. {@code 7u76}
     * @param build   e.g. {@code b13}
     */
    public static JreLoader forCurrendOS(String type, String version, String build) {
        String osName = System.getProperty("os.name");
        String osArch = System.getProperty("os.arch");

        return forOsNameAndArch(type, version, build, osName, osArch);
    }

    public static JreLoader forOsNameAndArch(String type, String version, String build, String osName, String osArch) {
        String os = osFromSystemProperty(osName);

        String arch = archFromSystemProperty(osArch);

        return new JreLoader(type, version, build, os, arch, "tar.gz");
    }

    public static String osFromSystemProperty() {
        return osFromSystemProperty(System.getProperty("os.name"));
    }

    public static String osFromSystemProperty(String osName) {
        osName = osName.toLowerCase();
        String os;
        if (osName.contains("mac") || osName.contains("darwin")) {
            os = "macosx";
        } else if (osName.contains("linux")) {
            os = "linux";
        } else {
            throw new IllegalArgumentException("Unknown OS " + osName);
        }
        return os;
    }

    public static String archFromSystemProperty() {
        return archFromSystemProperty(System.getProperty("os.arch"));
    }

    public static String archFromSystemProperty(String osArch) {
        osArch = osArch.toLowerCase();
        String arch;
        switch (osArch) {
            // TODO do we want 32bit architectures in Mesos ?
            case "x86":
            case "i386":
            case "i586":
                arch = "i586";
                break;
            case "x86_64":
            case "amd64":
                arch = "x64";
                break;
            default:
                arch = osArch;
        }
        return arch;
    }

    public String getArch() {
        return arch;
    }

    public String getBuild() {
        return build;
    }

    public String getExtension() {
        return extension;
    }

    public String getOs() {
        return os;
    }

    public static String getPrefix() {
        return prefix;
    }

    public String getType() {
        return type;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String getFilename() {
        return type + '-' + version + '-' + os + '-' + arch + '.' + extension;
    }

    @Override
    protected String getDownloadURL() {
        return prefix + version + '-' + build + '/' + getFilename();
    }

    protected void updateURLConnection(HttpURLConnection urlConn) {
        urlConn.addRequestProperty("Cookie", cookie);
    }

}
