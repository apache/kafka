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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.jose4j.jwk.HttpsJwks;
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

public final class RefreshingHttpsJwks extends HttpsJwks implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwks.class);

    private static final int SHUTDOWN_TIMEOUT = 10;

    private static final TimeUnit SHUTDOWN_TIME_UNIT = TimeUnit.SECONDS;

    private final ScheduledExecutorService executorService;

    private final long refreshIntervalMs;

    /**
     *
     * @param location          HTTP/HTTPS endpoint from which to retrieve the JWKS based on
     *                          the OAuth/OIDC standard
     * @param refreshIntervalMs The number of milliseconds between refresh passes to connect
     *                          to the OAuth/OIDC JWKS endpoint to retrieve the latest set
     */

    public RefreshingHttpsJwks(String location, long refreshIntervalMs) {
        super(location);

        if (refreshIntervalMs <= 0)
            throw new IllegalArgumentException("JWKS validation key refresh configuration value retryWaitMs value must be positive");

        setDefaultCacheDuration(refreshIntervalMs);

        this.refreshIntervalMs = refreshIntervalMs;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void init() {
        try {
            log.debug("initialization started");
            executorService.scheduleAtFixedRate(this::refreshInternal,
                0,
                refreshIntervalMs,
                TimeUnit.MILLISECONDS);
            log.info("JWKS validation key refresh thread started with a refresh interval of {} ms", refreshIntervalMs);
        } finally {
            log.debug("initialization completed");
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
     * Internal method that will refresh the cache and if errors are encountered, re-queues
     * the refresh attempt in a background thread.
     */

    private void refreshInternal() {
        try {
            log.info("JWKS validation key refresh processing");

            // Call the *actual* refresh implementation.
            refresh();
        } catch (JoseException | IOException e) {
            // Let's wait a random, but short amount of time before trying again.
            long waitMs = ThreadLocalRandom.current().nextLong(1000, 10000);

            String message = String.format("JWKS validation key refresh encountered an error connecting to %s; waiting %s ms before trying again",
                getLocation(),
                waitMs);
            log.warn(message, e);

            executorService.schedule(this::refreshInternal, waitMs, TimeUnit.MILLISECONDS);
        }
    }

}
