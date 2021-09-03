/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.oauthbearer.secured;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>FileWatchService</code> allows the user to register a callback for file events on
 * a given directory. The embedded {@link WatchService} looks for all events defined in the
 * {@link StandardWatchEventKinds} types and will call a corresponding callback method in
 * {@link FileWatchCallback} with the {@link Path} that triggered the event.
 *
 * @see FileWatchCallback
 * @see Path
 * @see StandardWatchEventKinds
 * @see WatchService
 */

public class FileWatchService implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileWatchService.class);

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS;

    // A polling service is used if the file system doesn't support watching, e.g. on MacOS.
    private static final String POLLING_WATCH_SERVICE_CLASS_NAME = "sun.nio.fs.PollingWatchService";
    private static final String SENSITIVITY_MODIFIER_CLASS_NAME = "com.sun.nio.file.SensitivityWatchEventModifier";
    private static final boolean USES_POLLING_WATCH_SERVICE;
    // Minimum interval between file updates for watch service to detect changes. More frequent
    // updates may not be notified, e.g. on MacOS where file modification time doesn't have
    // millisecond resolution. This is used in tests that rely on update notification.
    public static final Duration MIN_WATCH_INTERVAL;

    private static WatchEvent.Modifier watchEventModifier = null;

    private final Path directory;

    private final FileWatchCallback callback;

    private final String logPrefix;

    private final ReentrantReadWriteLock lock;

    private ExecutorService executorService;

    private WatchService watchService;

    static {
        boolean usesPollingService = false;

        try (WatchService service = FileSystems.getDefault().newWatchService()) {
            usesPollingService = service.getClass().getName().equals(POLLING_WATCH_SERVICE_CLASS_NAME);
        } catch (Throwable t) {
            log.error("Could not determine watch service type");
        }

        USES_POLLING_WATCH_SERVICE = usesPollingService;
        MIN_WATCH_INTERVAL = usesPollingService ? Duration.ofSeconds(3) : Duration.ZERO;
    }

    public FileWatchService(Path directory, FileWatchCallback callback) throws IOException {
        directory = directory.toAbsolutePath();

        if (!directory.toFile().isDirectory())
            throw new IOException(String.format("Path %s must be a directory", directory));

        this.directory = directory;
        this.callback = callback;
        this.logPrefix = String.format("File watch service for %s", directory);
        this.lock = new ReentrantReadWriteLock();
    }

    public void init() throws IOException {
        try {
            log.debug("{} initialization started", logPrefix);

            lock.writeLock().lock();

            watchService = FileSystems.getDefault().newWatchService();
            executorService = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "file-watch-service");
                thread.setDaemon(true);
                return thread;
            });

            Kind<?>[] events = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY, OVERFLOW};

            if (watchEventModifier == null)
                directory.register(watchService, events);
            else
                directory.register(watchService, events, watchEventModifier);

            executorService.submit(this::watch);
        } finally {
            lock.writeLock().unlock();

            log.debug("{} initialization completed", logPrefix);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            log.debug("{} close started", logPrefix);

            lock.writeLock().lock();

            if (executorService != null) {
                try {
                    executorService.shutdownNow();

                    if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT)) {
                        log.warn("{} thread termination did not end after {} {}",
                            logPrefix, SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT);
                    }
                } catch (InterruptedException e) {
                    log.warn(String.format("%s encountered error during close", logPrefix), e);
                } finally {
                    executorService = null;
                }
            }

            if (watchService != null) {
                try {
                    watchService.close();
                } catch (IOException e) {
                    log.warn(String.format("%s encountered error during close", logPrefix), e);
                } finally {
                    watchService = null;
                }
            }
        } finally {
            lock.writeLock().unlock();

            log.debug("{} close completed", logPrefix);
        }
    }

    // Only for test usage. This reduces polling interval from default of 10s to 2s to
    // avoid long delays in tests.
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void useHighSensitivity() {
        try {
            if (USES_POLLING_WATCH_SERVICE) {
                Class<?> modifierClass = Class.forName(SENSITIVITY_MODIFIER_CLASS_NAME);
                watchEventModifier = (WatchEvent.Modifier) Enum.valueOf((Class<Enum>) modifierClass, "HIGH");
            }
        } catch (Exception e) {
            throw new KafkaException("Could not configure high sensitivity");
        }
    }

    public static void resetSensitivity() {
        watchEventModifier = null;
    }

    private void watch() {
        log.debug("{} watch loop started", logPrefix);

        while (true) {
            WatchService localWatchService;

            {
                // Grab a local copy of the WatchService so we don't have to keep a lock too long.
                try {
                    lock.readLock().lock();
                    localWatchService = watchService;
                } finally {
                    lock.readLock().unlock();
                }

                if (localWatchService == null) {
                    warnStopPolling(null, "watch service shutting down", null);
                    break;
                }
            }

            try {
                WatchKey watchKey = localWatchService.take();

                if (!watchKey.isValid()) {
                    warnStopPolling(watchKey, "watch key was not valid", null);
                    break;
                }

                for (WatchEvent<?> event : watchKey.pollEvents()) {
                    Path p;

                    {
                        @SuppressWarnings("unchecked")
                        WatchEvent<Path> e = (WatchEvent<Path>) event;

                        if (e.context() == null)
                            continue;

                        p = e.context().getFileName();

                        if (!p.isAbsolute())
                            p = directory.resolve(p);
                    }

                    try {
                        if (event.kind() == ENTRY_CREATE) {
                            log.debug("{} notified of {} file creation", logPrefix, p);
                            callback.fileCreated(p);
                        } else if (event.kind() == ENTRY_MODIFY || event.kind() == OVERFLOW) {
                            log.debug("{} notified of {} file modification", logPrefix, p);
                            callback.fileModified(p);
                        } else if (event.kind() == ENTRY_DELETE) {
                            log.warn("{} notified of {} file deletion", logPrefix, p);
                            callback.fileDeleted(p);
                        } else {
                            log.error("{} notified of {} file unknown event {}", logPrefix, p, event.kind());
                        }
                    } catch (Exception e) {
                        log.error(String.format("%s encountered an error during callback for %s", logPrefix, p), e);
                    }
                }

                if (!watchKey.reset()) {
                    warnStopPolling(watchKey, "watch key cannot be reset", null);
                    break;
                }
            } catch (InterruptedException e) {
                warnStopPolling(null, "watch service shutting down", null);
                break;
            } catch (Exception e) {
                warnStopPolling(null, e.getMessage(), e);
                break;
            }
        }

        log.debug("{} watch loop completed", logPrefix);
    }

    private void warnStopPolling(WatchKey watchKey, String reason, Exception e) {
        String s = String.format("%s: %s; refresh will stop watching for events", logPrefix, reason);

        if (e != null)
            log.warn(s, e);
        else
            log.warn(s);

        if (watchKey != null) {
            watchKey.cancel();
            log.warn("{} canceled watch key", logPrefix);
        }
    }

    /**
     * Simple callback mechanism for when files are created, deleted, or modified.
     */

    public interface FileWatchCallback {

        void fileCreated(Path path) throws IOException;

        void fileDeleted(Path path) throws IOException;

        void fileModified(Path path) throws IOException;

    }

}
