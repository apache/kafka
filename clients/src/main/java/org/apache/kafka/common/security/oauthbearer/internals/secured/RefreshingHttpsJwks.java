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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

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

public final class RefreshingHttpsJwks extends HttpsJwks implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwks.class);

    private final ScheduledExecutorService scheduler;

    public RefreshingHttpsJwks(String location, long refreshIntervalMs) {
        super(location);

        if (refreshIntervalMs <= 0)
            throw new IllegalArgumentException("JWKS validation key refresh configuration value retryWaitMs value must be positive");

        setDefaultCacheDuration(refreshIntervalMs);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::refreshInternal,
            0,
            refreshIntervalMs,
            TimeUnit.MILLISECONDS);
        log.info("JWKS validation key refresh thread started with a refresh interval of {} ms", refreshIntervalMs);
    }

    @Override
    public void close() {
        if (isRunning()) {
            log.info("JWKS validation key refresh thread shutting down");
            scheduler.shutdownNow();
        }
    }

    public boolean isRunning() {
        return scheduler != null && !scheduler.isShutdown();
    }

    private void refreshInternal() {
        try {
            log.info("JWKS validation key refresh processing");

            // Call the *actual* refresh() implementation.
            super.refresh();
        } catch (JoseException | IOException e) {
            // Let's wait a random, but short amount of time before trying again.
            long waitMs = ThreadLocalRandom.current().nextLong(100, 1000);

            String message = String.format("JWKS validation key refresh encountered an error connecting to %s; waiting %s ms before trying again",
                getLocation(),
                waitMs);
            log.warn(message, e);

            scheduler.schedule(this::refreshInternal, waitMs, TimeUnit.MILLISECONDS);
        }
    }

}
