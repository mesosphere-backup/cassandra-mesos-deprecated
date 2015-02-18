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
package io.mesosphere.mesos.frameworks.cassandra.files;

import com.google.common.collect.Maps;
import io.mesosphere.mesos.frameworks.cassandra.bindown.Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of a simple, stupid caching proxy for externally provided binaries.
 */
public final class CachedFileResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedFileResource.class);

    private CachedFileResource() {
    }

    static final File fileCacheDir;

    static final AtomicInteger statsDownloads = new AtomicInteger();
    static final AtomicLong statsDownloadedBytes = new AtomicLong();
    static final AtomicLong statsLastDownload = new AtomicLong();

    static {
        File dir = new File(System.getProperty("user.dir"), "file-cache");
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new RuntimeException("Cannot use directory " + dir.getAbsolutePath() + " as file cache");
        fileCacheDir = dir;

//        Runtime.getRuntime().addShutdownHook(new Thread("cached-files-cleanup") {
//            @Override
//            public void run() {
//                File[] files = fileCacheDir.listFiles();
//                if (files != null)
//                    for (File file : files)
//                        file.delete();
//                fileCacheDir.delete();
//            }
//        });
    }

    private static final ConcurrentMap<String, CachedFile> cache = Maps.newConcurrentMap();

    static Response serveCached(Downloader downloader, File provided) {
        if (provided != null)
            return serveFile(downloader, provided);

        CachedFile cachedFile = cache.get(downloader.getFilename());
        if (cachedFile == null) {
            CachedFile existing = cache.putIfAbsent(downloader.getFilename(), cachedFile = new CachedFile(downloader));
            if (existing != null)
                cachedFile = existing;
        }

        return cachedFile.serve();
    }

    static Response serveFile(Downloader downloader, File file) {
        final Response.ResponseBuilder builder = Response.ok(file, "application/x-gzip");
        builder.header("Content-Disposition", String.format("attachment; filename=\"%s\"", downloader.getFilename()));
        return builder.build();
    }

    private static class CachedFile {
        private final Downloader downloader;
        private File file;
        private final ReentrantLock lock = new ReentrantLock();

        CachedFile(Downloader downloader) {
            this.downloader = downloader;
        }

        public Response serve() {
            lock.lock();
            try {
                if (file == null || !file.canRead()) {
                    try {
                        File f = new File(fileCacheDir, downloader.getFilename());
                        if (!f.canRead()) {
                            LOGGER.info("Starting download to cache file {}", downloader.getFilename());
                            downloader.download(f);
                        } else
                            LOGGER.info("Using aready downloaded cache file {}", downloader.getFilename());
                        file = f;
                        statsLastDownload.set(System.currentTimeMillis());
                        statsDownloads.incrementAndGet();
                        statsDownloadedBytes.addAndGet(f.length());
                    } catch (IOException e) {
                        LOGGER.error("Failed to download " + downloader.getFilename(), e);
                        return Response.serverError().build();
                    }
                }

                LOGGER.info("Serving cached file {}", downloader.getFilename());
                return serveFile(downloader, file);
            } finally {
                lock.unlock();
            }
        }
    }

    public static long getStatsDownloadedBytes() {
        return statsDownloadedBytes.get();
    }

    public static int getStatsDownloads() {
        return statsDownloads.get();
    }

    public static long getStatsLastDownload() {
        return statsLastDownload.get();
    }
}
