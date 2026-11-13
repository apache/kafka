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
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.BootstrapResolutionException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.internals.ThreadUtils;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Resolves the configured bootstrap servers without blocking the network client event loop.
 *
 * <p>The resolver owns the asynchronous DNS operation and its timeout/retry state. The caller remains responsible
 * for deciding whether metadata still needs to be bootstrapped and for applying the resolved addresses.</p>
 *
 * <p>This class is not thread-safe. {@link #ensureBootstrapped(long, MetadataUpdater)} must be called from the
 * client's event loop.</p>
 */
final class BootstrapServerResolver implements AutoCloseable {
    @FunctionalInterface
    interface AddressResolver {
        List<InetSocketAddress> resolve();
    }

    private final BootstrapConfiguration configuration;
    private final Time time;
    private final Logger log;
    private final AddressResolver addressResolver;
    private final ExecutorService executor;

    private Timer timer;
    private CompletableFuture<List<InetSocketAddress>> pendingResolution;
    private long retryAtMs = -1L;
    private BootstrapResolutionException exception;

    BootstrapServerResolver(BootstrapConfiguration configuration, Time time, LogContext logContext) {
        this(configuration, time, logContext, () -> ClientUtils.parseAddresses(
            configuration.bootstrapServers,
            configuration.clientDnsLookup));
    }

    BootstrapServerResolver(BootstrapConfiguration configuration,
                            Time time,
                            LogContext logContext,
                            AddressResolver addressResolver) {
        this.configuration = configuration;
        this.time = time;
        this.log = logContext.logger(BootstrapServerResolver.class);
        this.addressResolver = Objects.requireNonNull(addressResolver, "addressResolver must not be null");

        if (configuration == BootstrapConfiguration.DISABLED) {
            this.executor = null;
        } else {
            this.executor = Executors.newSingleThreadExecutor(
                ThreadUtils.createThreadFactory("kafka-bootstrap-dns-resolver", true));
            startResolution();
        }
    }

    void ensureBootstrapped(final long currentTimeMs, final MetadataUpdater metadataUpdater) {
        if (configuration == BootstrapConfiguration.DISABLED || metadataUpdater.isBootstrapped() || exception != null)
            return;

        if (Thread.interrupted()) {
            cancelResolution();
            throw new InterruptException(new InterruptedException());
        }

        // Start the timer on the first poll so time spent constructing an idle client does not consume the timeout.
        if (timer == null)
            timer = time.timer(configuration.bootstrapResolveTimeoutMs);

        // Process a result before checking the timeout so a result completed at the deadline is accepted.
        if (maybeProcessResolutionResult(currentTimeMs, metadataUpdater))
            return;

        timer.update(currentTimeMs);
        checkTimeout(metadataUpdater);
        maybeStartResolution(currentTimeMs);
    }

    private void startResolution() {
        pendingResolution = CompletableFuture.supplyAsync(addressResolver::resolve, executor);
    }

    private void cancelResolution() {
        if (pendingResolution != null) {
            pendingResolution.cancel(true);
            pendingResolution = null;
        }
        retryAtMs = -1L;
    }

    private void checkTimeout(MetadataUpdater metadataUpdater) {
        if (timer.isExpired() && exception == null) {
            cancelResolution();
            exception = new BootstrapResolutionException("Failed to resolve bootstrap servers after "
                + configuration.bootstrapResolveTimeoutMs + "ms. Please check your bootstrap.servers configuration "
                + "and DNS settings.");
            metadataUpdater.bootstrapFailed(exception);
        }
    }

    private void maybeStartResolution(final long currentTimeMs) {
        if (exception != null || pendingResolution != null)
            return;

        if (retryAtMs >= 0 && currentTimeMs < retryAtMs)
            return;

        retryAtMs = -1L;
        startResolution();
    }

    private boolean maybeProcessResolutionResult(final long currentTimeMs, MetadataUpdater metadataUpdater) {
        if (pendingResolution == null || !pendingResolution.isDone())
            return false;

        List<InetSocketAddress> servers = List.of();
        try {
            servers = pendingResolution.getNow(List.of());
        } catch (CompletionException e) {
            log.debug("DNS resolution failed", e);
        }
        pendingResolution = null;

        if (!servers.isEmpty()) {
            log.debug("Bootstrap DNS resolution succeeded, {} servers resolved", servers.size());
            metadataUpdater.bootstrap(servers);
            return true;
        }

        log.debug("Failed to resolve bootstrap servers, will retry after {}ms. Remaining time: {}ms",
            configuration.retryBackoffMs, timer.remainingMs());
        retryAtMs = currentTimeMs + configuration.retryBackoffMs;
        return false;
    }

    @Override
    public void close() {
        cancelResolution();
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 1, TimeUnit.SECONDS);
    }
}
