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

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link HttpsJwks} that will periodically refresh the JWKS cache to reduce or
 * even prevent HTTP/HTTPS traffic in the hot path of validation. It is assumed that it's
 * possible to receive a JWT that contains a <code>kid</code> that points to yet-unknown JWK,
 * thus requiring a connection to the OAuth/OIDC provider to be made. Hopefully, in practice,
 * keys are made available for some amount of time before they're used within JWTs.
 *
 * This instance is created and provided to the
 * {@link org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver} that is used when using
 * an HTTP-/HTTPS-based {@link org.jose4j.keys.resolvers.VerificationKeyResolver}, which is then
 * provided to the {@link ValidatorAccessTokenValidator} to use in validating the signature of
 * a JWT.
 *
 * @see org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver
 * @see org.jose4j.keys.resolvers.VerificationKeyResolver
 * @see ValidatorAccessTokenValidator
 */

public final class RefreshingHttpsJwks extends HttpsJwks implements Initable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwks.class);

    private static final int MISSING_KEY_ID_CACHE_MAX_ENTRIES = 16;

    private static final long MISSING_KEY_ID_CACHE_IN_FLIGHT_MS = 60000;

    private static final int MISSING_KEY_ID_MAX_KEY_LENGTH = 1000;

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS;

    private final ScheduledExecutorService executorService;

    private final long refreshMs;

    private final ReadWriteLock refreshLock = new ReentrantReadWriteLock();

    private final Map<String, Long> missingKeyIds;

    private List<JsonWebKey> jsonWebKeys;

    private boolean isInited;

    /**
     *
     * @param location  HTTP/HTTPS endpoint from which to retrieve the JWKS based on
     *                  the OAuth/OIDC standard
     * @param refreshMs The number of milliseconds between refresh passes to connect
     *                  to the OAuth/OIDC JWKS endpoint to retrieve the latest set
     */

    public RefreshingHttpsJwks(String location, long refreshMs) {
        super(location);

        if (refreshMs <= 0)
            throw new IllegalArgumentException("JWKS validation key refresh configuration value retryWaitMs value must be positive");

        setDefaultCacheDuration(refreshMs);

        this.refreshMs = refreshMs;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.missingKeyIds = new LinkedHashMap<String, Long>(MISSING_KEY_ID_CACHE_MAX_ENTRIES, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return this.size() > MISSING_KEY_ID_CACHE_MAX_ENTRIES;
            }
        };
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("init started");

            List<JsonWebKey> localJWKs;

            try {
                localJWKs = super.getJsonWebKeys();
            } catch (JoseException e) {
                throw new IOException("Could not refresh JWKS", e);
            }

            try {
                refreshLock.writeLock().lock();
                this.jsonWebKeys = localJWKs;
            } finally {
                refreshLock.writeLock().unlock();
            }

            executorService.scheduleAtFixedRate(this::refreshInternal,
                0,
                refreshMs,
                TimeUnit.MILLISECONDS);

            log.info("JWKS validation key refresh thread started with a refresh interval of {} ms", refreshMs);
        } finally {
            isInited = true;

            log.debug("init completed");
        }
    }

    @Override
    public void close() {
        try {
            log.debug("close started");

            try {
                log.debug("JWKS validation key refresh thread shutting down");
                executorService.shutdown();

                if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT)) {
                    log.warn("JWKS validation key refresh thread termination did not end after {} {}",
                        SHUTDOWN_TIMEOUT, SHUTDOWN_TIME_UNIT);
                }
            } catch (InterruptedException e) {
                log.warn("JWKS validation key refresh thread error during close", e);
            }
        } finally {
            log.debug("close completed");
        }
    }

    @Override
    public List<JsonWebKey> getJsonWebKeys() throws JoseException, IOException {
        if (!isInited)
            throw new IllegalStateException("Please call init() first");

        try {
            refreshLock.readLock().lock();
            return jsonWebKeys;
        } finally {
            refreshLock.readLock().unlock();
        }
    }

    /**
     * Internal method that will refresh the cache and if errors are encountered, re-queues
     * the refresh attempt in a background thread.
     */

    private void refreshInternal() {
        try {
            log.info("JWKS validation key refresh of {} starting", getLocation());

            // Call the *actual* refresh implementation.
            refresh();

            List<JsonWebKey> jwks = getJsonWebKeys();

            try {
                refreshLock.writeLock().lock();

                for (JsonWebKey jwk : jwks)
                    missingKeyIds.remove(jwk.getKeyId());
            } finally {
                refreshLock.writeLock().unlock();
            }

            log.info("JWKS validation key refresh of {} complete", getLocation());
        } catch (JoseException | IOException e) {
            // Let's wait a random, but short amount of time before trying again.
            long waitMs = ThreadLocalRandom.current().nextLong(1000, 10000);

            String message = String.format("JWKS validation key refresh of %s encountered an error; waiting %s ms before trying again",
                getLocation(),
                waitMs);
            log.warn(message, e);

            executorService.schedule(this::refreshInternal, waitMs, TimeUnit.MILLISECONDS);
        }
    }

    public boolean maybeScheduleRefreshForMissingKeyId(String keyId) {
        if (keyId.length() > MISSING_KEY_ID_MAX_KEY_LENGTH) {
            log.warn("Key ID starting with {} with length {} was too long to cache", keyId.substring(0, 16), keyId.length());
            return false;
        } else {
            try {
                refreshLock.writeLock().lock();

                // If there's no entry in the missing key ID cache for the incoming key ID,
                // or it has expired, schedule a refresh.
                Long lastCheckTime = missingKeyIds.get(keyId);
                long currTime = System.currentTimeMillis();

                if (lastCheckTime == null || lastCheckTime < currTime) {
                    lastCheckTime = currTime + MISSING_KEY_ID_CACHE_IN_FLIGHT_MS;
                    missingKeyIds.put(keyId, lastCheckTime);
                    executorService.schedule(this::refreshInternal, 0, TimeUnit.MILLISECONDS);
                    return true;
                } else {
                    return false;
                }
            } finally {
                refreshLock.writeLock().unlock();
            }
        }
    }

}
