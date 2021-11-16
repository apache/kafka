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
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwks.class);

    private static final int MISSING_KEY_ID_CACHE_MAX_ENTRIES = 16;

    static final long MISSING_KEY_ID_CACHE_IN_FLIGHT_MS = 60000;

    static final int MISSING_KEY_ID_MAX_KEY_LENGTH = 1000;

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS;

    /**
     * {@link HttpsJwks} does the actual work of contacting the OAuth/OIDC endpoint to get the
     * JWKS. In some cases, the call to {@link HttpsJwks#getJsonWebKeys()} will trigger a call
     * to {@link HttpsJwks#refresh()} which will block the current thread in network I/O. We cache
     * the JWKS ourselves (see {@link #jsonWebKeys}) to avoid the network I/O.
     *
     * We want to be very careful where we use the {@link HttpsJwks} instance so that we don't
     * perform any operation (directly or indirectly) that could cause blocking. This is because
     * the JWKS logic is part of the larger authentication logic which operates on Kafka's network
     * thread. It's OK to execute {@link HttpsJwks#getJsonWebKeys()} (which calls
     * {@link HttpsJwks#refresh()}) from within {@link #init()} as that method is called only at
     * startup, and we can afford the blocking hit there.
     */

    private final HttpsJwks httpsJwks;

    private final ScheduledExecutorService executorService;

    private final Time time;

    private final long refreshMs;

    private final long refreshRetryBackoffMs;

    private final long refreshRetryBackoffMaxMs;

    /**
     * Protects {@link #missingKeyIds} and {@link #jsonWebKeys}.
     */

    private final ReadWriteLock refreshLock = new ReentrantReadWriteLock();

    private final Map<String, Long> missingKeyIds;

    /**
     * Flag to prevent concurrent refresh invocations.
     */

    private final AtomicBoolean refreshInProgressFlag = new AtomicBoolean(false);

    /**
     * As mentioned in the comments for {@link #httpsJwks}, we cache the JWKS ourselves so that
     * we can return the list immediately without any network I/O. They are only cached within
     * calls to {@link #refresh()}.
     */

    private List<JsonWebKey> jsonWebKeys;

    private boolean isInitialized;

    /**
     * Creates a <code>RefreshingHttpsJwks</code> that will be used by the
     * {@link RefreshingHttpsJwksVerificationKeyResolver} to resolve new key IDs in JWTs.
     *
     * @param time                     {@link Time} instance
     * @param httpsJwks                {@link HttpsJwks} instance from which to retrieve the JWKS
     *                                 based on the OAuth/OIDC standard
     * @param refreshMs                The number of milliseconds between refresh passes to connect
     *                                 to the OAuth/OIDC JWKS endpoint to retrieve the latest set
     * @param refreshRetryBackoffMs    Time for delay after initial failed attempt to retrieve JWKS
     * @param refreshRetryBackoffMaxMs Maximum time to retrieve JWKS
     */

    public RefreshingHttpsJwks(Time time,
        HttpsJwks httpsJwks,
        long refreshMs,
        long refreshRetryBackoffMs,
        long refreshRetryBackoffMaxMs) {
        if (refreshMs <= 0)
            throw new IllegalArgumentException("JWKS validation key refresh configuration value retryWaitMs value must be positive");

        this.httpsJwks = httpsJwks;
        this.time = time;
        this.refreshMs = refreshMs;
        this.refreshRetryBackoffMs = refreshRetryBackoffMs;
        this.refreshRetryBackoffMaxMs = refreshRetryBackoffMaxMs;
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

            // Since we just grabbed the keys (which will have invoked a HttpsJwks.refresh()
            // internally), we can delay our first invocation by refreshMs.
            //
            // Note: we refer to this as a _scheduled_ refresh.
            executorService.scheduleAtFixedRate(this::refresh,
                refreshMs,
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
     * Our implementation avoids the blocking call within {@link HttpsJwks#refresh()} that is
     * sometimes called internal to {@link HttpsJwks#getJsonWebKeys()}. We want to avoid any
     * blocking I/O as this code is running in the authentication path on the Kafka network thread.
     *
     * The list may be stale up to {@link #refreshMs}.
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
     * <p>
     * <code>refresh</code> is an internal method that will refresh the JWKS cache and is
     * invoked in one of two ways:
     *
     * <ol>
     *     <li>Scheduled</li>
     *     <li>Expedited</li>
     * </ol>
     * </p>
     *
     * <p>
     * The <i>scheduled</i> refresh is scheduled in {@link #init()} and runs every
     * {@link #refreshMs} milliseconds. An <i>expedited</i> refresh is performed when an
     * incoming JWT refers to a key ID that isn't in our JWKS cache ({@link #jsonWebKeys})
     * and we try to perform a refresh sooner than the next scheduled refresh.
     * </p>
     */

    private void refresh() {
        if (!refreshInProgressFlag.compareAndSet(false, true)) {
            log.debug("OAuth JWKS refresh is already in progress; ignoring concurrent refresh");
            return;
        }

        try {
            log.info("OAuth JWKS refresh of {} starting", httpsJwks.getLocation());
            Retry<List<JsonWebKey>> retry = new Retry<>(refreshRetryBackoffMs, refreshRetryBackoffMaxMs);
            List<JsonWebKey> localJWKs = retry.execute(() -> {
                try {
                    log.debug("JWKS validation key calling refresh of {} starting", httpsJwks.getLocation());
                    // Call the *actual* refresh implementation that will more than likely issue
                    // HTTP(S) calls over the network.
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
        } finally {
            refreshInProgressFlag.set(false);
        }
    }

    /**
     * <p>
     * <code>maybeExpediteRefresh</code> is a public method that will trigger a refresh of
     * the JWKS cache if all of the following conditions are met:
     *
     * <ul>
     *     <li>The given <code>keyId</code> parameter is &lte; the
     *     {@link #MISSING_KEY_ID_MAX_KEY_LENGTH}</li>
     *     <li>The key isn't in the process of being expedited already</li>
     * </ul>
     *
     * <p>
     * This <i>expedited</i> refresh is scheduled immediately.
     * </p>
     *
     * @param keyId JWT key ID
     * @return <code>true</code> if an expedited refresh was scheduled, <code>false</code> otherwise
     */

    public boolean maybeExpediteRefresh(String keyId) {
        if (keyId.length() > MISSING_KEY_ID_MAX_KEY_LENGTH) {
            // Although there's no limit on the length of the key ID, they're generally
            // "reasonably" short. If we have a very long key ID length, we're going to assume
            // the JWT is malformed, and we will not actually try to resolve the key.
            //
            // In this case, let's prevent blowing out our memory in two ways:
            //
            //     1. Don't try to resolve the key as the large ID will sit in our cache
            //     2. Report the issue in the logs but include only the first N characters
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
                    // or it has expired, schedule a refresh ASAP.
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
