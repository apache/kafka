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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.utils.ConfigUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Polls a set of SSL material files (keystore, truststore) at a fixed interval and notifies
 * all registered listeners when a modification is detected.
 *
 * <h3>Debounce</h3>
 * During certificate rotation an operator may update the keystore and the truststore with some
 * delay between them. Without a grace period the poller would fire between the two writes,
 * loading a half-updated, inconsistent TLS configuration that can cause transient handshake
 * failures.
 *
 * <p>When {@code ssl.hotreload.debounce.seconds} is greater than zero the poller waits that long
 * after detecting the <em>first</em> changed file before notifying listeners. If another file
 * change arrives within the debounce window the timer is reset, so listeners are always called
 * after a quiet period — once both files have been written and the filesystem has settled.
 *
 * <p>Setting {@code ssl.hotreload.debounce.seconds=0} disables debouncing entirely and restores
 * the immediate-fire behaviour.
 *
 * <h3>Sharing</h3>
 * A single instance may be shared by multiple {@link SslFactory} instances that watch the same
 * set of files (see {@link SslMaterialPollerRegistry}). Listeners are managed via
 * {@link #addListener(Runnable)} and {@link #removeListener(Runnable)}.
 */
public final class SslMaterialPoller {

    private static final Logger log = LoggerFactory.getLogger(SslMaterialPoller.class);

    private final List<Path> files;
    private final Duration pollInterval;
    private final Duration debounceDelay;

    /** Thread-safe listener list; iterated after the debounce period expires. */
    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

    private final Map<Path, FileTime> lastModifiedTimes;

    private final ScheduledExecutorService scheduler;

    /**
     * Tracks the pending debounce task so it can be cancelled when a new change is detected
     * within the debounce window. Only ever accessed from the single scheduler thread.
     */
    private Future<?> pendingDebounce;

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Creates a poller for the keystore/truststore paths found in {@code configs}.
     * No listener is wired at construction time; use {@link #addListener(Runnable)}.
     *
     * @param configs Kafka SSL configuration map; must contain at least one of
     *                {@code ssl.keystore.location} or {@code ssl.truststore.location}.
     */
    public SslMaterialPoller(Map<String, Object> configs) {
        List<Path> files = new ArrayList<>();
        if (configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) != null) {
            files.add(Paths.get((String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)));
        }
        if (configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) != null) {
            files.add(Paths.get((String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)));
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one of ssl.keystore.location or ssl.truststore.location must be set");
        }

        this.files = files.stream()
                .map(f -> Objects.requireNonNull(f, "file path must not be null"))
                .collect(Collectors.toUnmodifiableList());

        this.pollInterval = Duration.ofSeconds(ConfigUtils.getInt(
                configs,
                SslConfigs.SSL_HOT_RELOAD_POLL_INTERVAL_CONFIG,
                SslConfigs.DEFAULT_SSL_HOT_RELOAD_POLL_INTERVAL_SECONDS));

        int debounceSeconds = ConfigUtils.getInt(
                configs,
                SslConfigs.SSL_HOT_RELOAD_DEBOUNCE_CONFIG,
                SslConfigs.DEFAULT_SSL_HOT_RELOAD_DEBOUNCE_SECONDS);
        this.debounceDelay = Duration.ofSeconds(debounceSeconds);

        this.lastModifiedTimes = new ConcurrentHashMap<>();

        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "ssl-material-poller");
            t.setDaemon(true);
            return t;
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(tf);

        log.debug("SslMaterialPoller created [files={}, pollInterval={}s, debounce={}s]",
                this.files, pollInterval.toSeconds(), debounceDelay.toSeconds());
    }

    // -------------------------------------------------------------------------
    // Listener management
    // -------------------------------------------------------------------------

    /**
     * Registers a listener to be called after the debounce period expires following a file change.
     * Safe to call at any time, including before {@link #start()}.
     */
    public void addListener(Runnable listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.add(listener);
        log.debug("Listener added [files={}, totalListeners={}]", files, listeners.size());
    }

    /**
     * Removes a previously registered listener. Uses reference equality.
     * No-op if the listener is not currently registered.
     */
    public void removeListener(Runnable listener) {
        boolean removed = listeners.remove(listener);
        if (removed) {
            log.debug("Listener removed [files={}, totalListeners={}]", files, listeners.size());
        }
    }

    /** Returns the current number of registered listeners. Intended for testing. */
    public int listenerCount() {
        return listeners.size();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the poller. Safe to call multiple times; the scheduling task is only registered once.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.debug("SslMaterialPoller already started for files {}", files);
            return;
        }

        log.debug("Starting SslMaterialPoller for files {}", files);

        for (Path file : files) {
            try {
                if (Files.exists(file)) {
                    FileTime lastModified = Files.getLastModifiedTime(file);
                    lastModifiedTimes.put(file, lastModified);
                    log.debug("Initial lastModified for {} is {}", file, lastModified);
                } else {
                    log.debug("Watched file {} does not exist at startup", file);
                }
            } catch (IOException e) {
                log.debug("Failed to read initial lastModified for {}", file, e);
                lastModifiedTimes.put(file, FileTime.fromMillis(0));
            }
        }

        scheduler.scheduleAtFixedRate(
                this::poll,
                pollInterval.toMillis(),
                pollInterval.toMillis(),
                TimeUnit.MILLISECONDS);

        log.debug("SslMaterialPoller scheduled [rate={}ms, debounce={}ms]",
                pollInterval.toMillis(), debounceDelay.toMillis());
    }

    /**
     * Stops the poller and shuts down the underlying scheduler. Any pending debounce task is
     * cancelled. Safe to call multiple times.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            log.debug("SslMaterialPoller not running (or already stopped) for files {}", files);
        } else {
            log.debug("Stopping SslMaterialPoller for files {}", files);
        }
        scheduler.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    /**
     * Called by the scheduler at every poll interval. Detects whether any watched file has been
     * modified since the last check. If a change is found and debouncing is enabled, schedules
     * (or reschedules) a one-shot notification task. If debouncing is disabled, notifies
     * listeners immediately.
     *
     * <p>All interactions with {@code pendingDebounce} happen on the single scheduler thread,
     * so no additional synchronization is needed for that field.
     */
    private void poll() {
        log.debug("Polling files {}", files);

        boolean changeDetected = false;

        for (Path file : files) {
            try {
                if (!Files.exists(file)) {
                    log.debug("File {} does not exist, skipping", file);
                    continue;
                }

                FileTime current = Files.getLastModifiedTime(file);
                FileTime previous = lastModifiedTimes.get(file);

                if (previous == null) {
                    log.debug("No previous lastModified for {}, recording {}", file, current);
                    lastModifiedTimes.put(file, current);
                    continue;
                }

                if (current.compareTo(previous) > 0) {
                    log.debug("Change detected for {} ({} -> {})", file, previous, current);
                    lastModifiedTimes.put(file, current);
                    changeDetected = true;
                    break; // one change is enough to launch the debounce; remaining files checked next cycle
                }

            } catch (IOException e) {
                log.debug("IOException while polling {}", file, e);
            } catch (RuntimeException e) {
                log.debug("Unexpected error while polling {}", file, e);
            }
        }

        if (!changeDetected) {
            log.trace("No changes detected");
            return;
        }

        if (debounceDelay.isZero() || files.size() == 1 ) {
            // Debouncing disabled so we notify immediately.
            log.debug("Change detected, debounce disabled - notifying listeners immediately");
            notifyListeners();
        } else {
            scheduleOrResetDebounce();
        }
    }

    /**
     * Cancels any pending debounce task and schedules a fresh one. Only ever called from the
     * single scheduler thread, so {@code pendingDebounce} needs no additional lock.
     */
    private void scheduleOrResetDebounce() {
        if (pendingDebounce != null && !pendingDebounce.isDone()) {
            pendingDebounce.cancel(false);
            log.debug("Debounce timer reset (new change detected within window) [debounce={}ms]",
                    debounceDelay.toMillis());
        } else {
            log.debug("Debounce timer armed [debounce={}ms]", debounceDelay.toMillis());
        }

        // Schedule a one-shot task on the same single-threaded scheduler so notifyListeners()
        // always runs on the poller thread, consistent with the no-debounce path.
        pendingDebounce = scheduler.schedule(
                this::notifyListeners,
                debounceDelay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /** Invokes all registered listeners, isolating each from exceptions thrown by others. */
    private void notifyListeners() {
        log.debug("Notifying {} listener(s)", listeners.size());
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                // Protect the scheduler thread and other listeners from a misbehaving one.
                log.warn("onChange listener threw an exception", e);
            }
        }
    }
}