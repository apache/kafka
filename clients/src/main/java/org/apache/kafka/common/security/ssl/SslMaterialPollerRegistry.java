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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A process-wide registry that ensures at most one {@link SslMaterialPoller} is created per
 * unique set of SSL material paths + poll interval.
 *
 * <h3>Motivation</h3>
 * A Kafka application typically configures SSL once, yet may create many consumers/producers,
 * each owning its own {@link SslFactory}. Without sharing, each factory would spawn its own
 * polling thread watching the same files — wasteful and noisy.
 * The registry solves this by deduplicating pollers: factories that share the same SSL paths
 * subscribe to the same underlying poller, while factories with distinct paths (e.g., connecting
 * to different clusters with different certificates) receive independent pollers.
 *
 * <h3>Lifecycle</h3>
 * A poller is started when its first subscriber registers and stopped (and removed from the
 * registry) when its last subscriber deregisters. All operations are synchronized on the registry
 * instance itself; this is intentionally coarse-grained since registration/deregistration are
 * infrequent operations.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // In SslFactory#configure():
 * this.onSslMaterialChange = () -> reconfigure(this.sslEngineFactoryConfig);
 * SslMaterialPollerRegistry.getInstance().register(configs, this.onSslMaterialChange);
 *
 * // In SslFactory#close():
 * SslMaterialPollerRegistry.getInstance().deregister(configs, this.onSslMaterialChange);
 * }</pre>
 */
public final class SslMaterialPollerRegistry {

    private static final Logger log = LoggerFactory.getLogger(SslMaterialPollerRegistry.class);

    private static final class Holder {
        static final SslMaterialPollerRegistry INSTANCE = new SslMaterialPollerRegistry();
    }

    public static SslMaterialPollerRegistry getInstance() {
        return Holder.INSTANCE;
    }

    // -------------------------------------------------------------------------
    // Registry key
    // -------------------------------------------------------------------------

    /**
     * Identifies a unique poller by the exact set of files it watches and its poll interval.
     * Two {@link SslFactory} instances are eligible to share a poller iff their keys are equal.
     */
    static final class PollerKey {
        private final String keystorePath;    // nullable
        private final String truststorePath;  // nullable
        private final int pollIntervalSeconds;
        private final int debounceSeconds;

        PollerKey(Map<String, Object> configs) {
            this.keystorePath = (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
            this.truststorePath = (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            this.pollIntervalSeconds = ConfigUtils.getInt(
                    configs,
                    SslConfigs.SSL_HOT_RELOAD_POLL_INTERVAL_CONFIG,
                    SslConfigs.DEFAULT_SSL_HOT_RELOAD_POLL_INTERVAL_SECONDS);
            this.debounceSeconds = ConfigUtils.getInt(
                    configs,
                    SslConfigs.SSL_HOT_RELOAD_DEBOUNCE_CONFIG,
                    SslConfigs.DEFAULT_SSL_HOT_RELOAD_DEBOUNCE_SECONDS);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PollerKey)) return false;
            PollerKey other = (PollerKey) o;
            return pollIntervalSeconds == other.pollIntervalSeconds
                    && debounceSeconds == other.debounceSeconds
                    && Objects.equals(keystorePath, other.keystorePath)
                    && Objects.equals(truststorePath, other.truststorePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keystorePath, truststorePath, pollIntervalSeconds, debounceSeconds);
        }

        @Override
        public String toString() {
            return "PollerKey{keystore=" + keystorePath
                    + ", truststore=" + truststorePath
                    + ", pollInterval=" + pollIntervalSeconds + "s"
                    + ", debounce=" + debounceSeconds + "s}";
        }
    }

    // -------------------------------------------------------------------------
    // Internal entry (poller + its active listeners)
    // -------------------------------------------------------------------------

    private static final class PollerEntry {
        final SslMaterialPoller poller;

        PollerEntry(Map<String, Object> configs) {
            this.poller = new SslMaterialPoller(configs);
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * Guarded by {@code this}. Plain HashMap is fine since all access is synchronized.
     * Using a HashMap rather than ConcurrentHashMap because the register/deregister
     * operations need a compound check-then-act that must be atomic.
     */
    private final Map<PollerKey, PollerEntry> entries = new HashMap<>();

    private SslMaterialPollerRegistry() {
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Registers {@code listener} as a subscriber for the SSL material paths described in
     * {@code configs}. If a poller for the same paths and interval already exists it is reused;
     * otherwise a new one is created and started.
     *
     * @param configs  Kafka SSL configuration map (read-only).
     * @param listener Callback invoked when a watched file changes. The caller must keep a
     *                 reference to the exact same {@code Runnable} instance to pass to
     *                 {@link #deregister} later.
     */
    public synchronized void register(Map<String, Object> configs, Runnable listener) {
        Objects.requireNonNull(configs, "configs must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        PollerKey key = new PollerKey(configs);
        PollerEntry entry = entries.computeIfAbsent(key, k -> {
            log.debug("Creating new SslMaterialPoller for {}", k);
            PollerEntry newEntry = new PollerEntry(configs);
            newEntry.poller.start();
            return newEntry;
        });

        entry.poller.addListener(listener);
        log.debug("Registered SSL material listener [key={}, totalListeners={}]",
                key, entry.poller.listenerCount());
    }

    /**
     * Removes {@code listener} from the poller identified by {@code configs}.
     * If this was the last listener for that poller, the poller is stopped and removed
     * from the registry, freeing its polling thread.
     *
     * <p>Uses reference equality to find the listener — pass the exact same {@code Runnable}
     * instance that was given to {@link #register}.
     *
     * @param configs  Kafka SSL configuration map (same as the one used in {@link #register}).
     * @param listener The exact listener instance previously passed to {@link #register}.
     */
    public synchronized void deregister(Map<String, Object> configs, Runnable listener) {
        Objects.requireNonNull(configs, "configs must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        PollerKey key = new PollerKey(configs);
        PollerEntry entry = entries.get(key);
        if (entry == null) {
            log.debug("deregister called for unknown key {}, nothing to do", key);
            return;
        }

        entry.poller.removeListener(listener);
        log.debug("Deregistered SSL material listener [key={}, remainingListeners={}]",
                key, entry.poller.listenerCount());

        if (entry.poller.listenerCount() == 0) {
            log.debug("No more listeners for {}, stopping and removing poller", key);
            entry.poller.stop();
            entries.remove(key);
        }
    }

    /**
     * Returns the number of active pollers in the registry. Intended for testing.
     */
    synchronized int pollerCount() {
        return entries.size();
    }

    /**
     * Returns the number of listeners registered against the poller for the given config,
     * or {@code 0} if no such poller exists. Intended for testing.
     */
    synchronized int listenerCount(Map<String, Object> configs) {
        PollerKey key = new PollerKey(configs);
        PollerEntry entry = entries.get(key);
        return entry == null ? 0 : entry.poller.listenerCount();
    }
}