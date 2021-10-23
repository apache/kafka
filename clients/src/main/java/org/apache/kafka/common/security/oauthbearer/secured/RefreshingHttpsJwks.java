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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.utils.Time;
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

public final class RefreshingHttpsJwks implements Initable, Closeable {

    private static final long RETRY_BACKOFF_MS = 2000;

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwks.class);

    private static final int MISSING_KEY_ID_CACHE_MAX_ENTRIES = 16;

    static final long MISSING_KEY_ID_CACHE_IN_FLIGHT_MS = 60000;

    static final int MISSING_KEY_ID_MAX_KEY_LENGTH = 1000;

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS;

    private final HttpsJwks httpsJwks;

    private final ScheduledExecutorService executorService;

    private ScheduledFuture<?> refreshFuture;

    private final Time time;

    private final long refreshMs;

    private final ReadWriteLock refreshLock = new ReentrantReadWriteLock();

    private final Map<String, Long> missingKeyIds;

    private List<JsonWebKey> jsonWebKeys;

    private boolean isInitialized;

    /**
     * Creates a <code>RefreshingHttpsJwks</code> that will be used by the
     * {@link RefreshingHttpsJwksVerificationKeyResolver} to resolve new key IDs in JWTs.
     *
     * @param time      {@link Time} instance
     * @param httpsJwks {@link HttpsJwks} instance from which to retrieve the JWKS based on
     *                  the OAuth/OIDC standard
     * @param refreshMs The number of milliseconds between refresh passes to connect
     *                  to the OAuth/OIDC JWKS endpoint to retrieve the latest set
     */

    public RefreshingHttpsJwks(Time time, HttpsJwks httpsJwks, long refreshMs) {
        if (refreshMs <= 0)
            throw new IllegalArgumentException("JWKS validation key refresh configuration value retryWaitMs value must be positive");

        this.httpsJwks = httpsJwks;
        this.time = time;
        this.refreshMs = refreshMs;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.missingKeyIds = new LinkedHashMap<String, Long>(MISSING_KEY_ID_CACHE_MAX_ENTRIES, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return this.size() > MISSING_KEY_ID_CACHE_MAX_ENTRIES;
            }
        };
    }

    /**
     * Creates a <code>RefreshingHttpsJwks</code> that will be used by the
     * {@link RefreshingHttpsJwksVerificationKeyResolver} to resolve new key IDs in JWTs.
     *
     * @param httpsJwks {@link HttpsJwks} instance from which to retrieve the JWKS based on
     *                  the OAuth/OIDC standard
     * @param refreshMs The number of milliseconds between refresh passes to connect
     *                  to the OAuth/OIDC JWKS endpoint to retrieve the latest set
     */

    public RefreshingHttpsJwks(HttpsJwks httpsJwks, long refreshMs) {
        this(Time.SYSTEM, httpsJwks, refreshMs);
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("init started");

            List<JsonWebKey> localJWKs;

            try {
                localJWKs = httpsJwks.getJsonWebKeys();
            } catch (JoseException e) {
                throw new IOException("Could not refresh JWKS", e);
            }

            try {
                refreshLock.writeLock().lock();
                jsonWebKeys = Collections.unmodifiableList(localJWKs);
            } finally {
                refreshLock.writeLock().unlock();
            }

            refreshFuture = executorService.scheduleAtFixedRate(this::refresh,
                0,
                refreshMs,
                TimeUnit.MILLISECONDS);

            log.info("JWKS validation key refresh thread started with a refresh interval of {} ms", refreshMs);
        } finally {
            isInitialized = true;

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

    /**
     * Overrides the base implementation because the base implementation has a case that performs
     * a blocking call to refresh(), which we want to avoid in the authentication validation path.
     *
     * The list may be stale up to refreshMs.
     *
     * @return {@link List} of {@link JsonWebKey} instances
     *
     * @throws JoseException Thrown if a problem is encountered parsing the JSON content into JWKs
     * @throws IOException Thrown f a problem is encountered making the HTTP request
     */

    public List<JsonWebKey> getJsonWebKeys() throws JoseException, IOException {
        if (!isInitialized)
            throw new IllegalStateException("Please call init() first");

        try {
            refreshLock.readLock().lock();
            return jsonWebKeys;
        } finally {
            refreshLock.readLock().unlock();
        }
    }

    public String getLocation() {
        return httpsJwks.getLocation();
    }

    /**
     * Internal method that will refresh the cache and if errors are encountered, re-queues
     * the refresh attempt in a background thread.
     */

    private void refresh() {
        // How much time (in milliseconds) do we have before the next refresh is scheduled to
        // occur? This value will [0..refreshMs]. Every time a scheduled refresh occurs, the
        // value of refreshFuture is reset to refreshMs and works down to 0.
        long timeBeforeNextRefresh = refreshFuture.getDelay(TimeUnit.MILLISECONDS);
        log.debug("timeBeforeNextRefresh: {}, RETRY_BACKOFF_MS: {}", timeBeforeNextRefresh, RETRY_BACKOFF_MS);

        // If the time left before the next scheduled refresh is less than the amount of time we
        // have set aside for retries, log the fact and return. Don't worry, refreshInternal will
        // be called again within a few seconds :)
        if (timeBeforeNextRefresh > 0 && timeBeforeNextRefresh < RETRY_BACKOFF_MS) {
            log.info("OAuth JWKS refresh does not have enough time before next scheduled refresh");
            return;
        }

        try {
            log.info("OAuth JWKS refresh of {} starting", httpsJwks.getLocation());
            Retry<List<JsonWebKey>> retry = new Retry<>(RETRY_BACKOFF_MS, timeBeforeNextRefresh);
            List<JsonWebKey> localJWKs = retry.execute(() -> {
                try {
                    log.debug("JWKS validation key calling refresh of {} starting", httpsJwks.getLocation());
                    // Call the *actual* refresh implementation.
                    httpsJwks.refresh();
                    List<JsonWebKey> jwks = httpsJwks.getJsonWebKeys();
                    log.debug("JWKS validation key refresh of {} complete", httpsJwks.getLocation());
                    return jwks;
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            });

            try {
                refreshLock.writeLock().lock();

                for (JsonWebKey jwk : localJWKs)
                    missingKeyIds.remove(jwk.getKeyId());

                jsonWebKeys = Collections.unmodifiableList(localJWKs);
            } finally {
                refreshLock.writeLock().unlock();
            }

            log.info("OAuth JWKS refresh of {} complete", httpsJwks.getLocation());
        } catch (ExecutionException e) {
            log.warn("OAuth JWKS refresh of {} encountered an error; not updating local JWKS cache", httpsJwks.getLocation(), e);
        }
    }

    public boolean maybeScheduleRefreshForMissingKeyId(String keyId) {
        if (keyId.length() > MISSING_KEY_ID_MAX_KEY_LENGTH) {
            // Only grab the first N characters so that if the key ID is huge, we don't blow up.
            int actualLength = keyId.length();
            String s = keyId.substring(0, MISSING_KEY_ID_MAX_KEY_LENGTH);
            String snippet = String.format("%s (trimmed to first %s characters out of %s total)", s, MISSING_KEY_ID_MAX_KEY_LENGTH, actualLength);
            log.warn("Key ID {} was too long to cache", snippet);
            return false;
        } else {
            try {
                refreshLock.writeLock().lock();

                Long nextCheckTime = missingKeyIds.get(keyId);
                long currTime = time.milliseconds();
                log.debug("For key ID {}, nextCheckTime: {}, currTime: {}", keyId, nextCheckTime, currTime);

                if (nextCheckTime == null || nextCheckTime <= currTime) {
                    // If there's no entry in the missing key ID cache for the incoming key ID,
                    // or it has expired, schedule a refresh.
                    nextCheckTime = currTime + MISSING_KEY_ID_CACHE_IN_FLIGHT_MS;
                    missingKeyIds.put(keyId, nextCheckTime);
                    executorService.schedule(this::refresh, 0, TimeUnit.MILLISECONDS);
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
