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
import org.apache.kafka.common.utils.internals.ThreadUtils;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Owns asynchronous bootstrap address resolution. The caller decides whether bootstrap is still
 * needed and applies resolution results to metadata; a successful resolution is not a terminal state.
 */
final class BootstrapResolver implements AutoCloseable {
    private final BootstrapConfiguration configuration;
    private final Time time;
    private final Logger log;
    private final ExecutorService executor;
    private final Supplier<CompletableFuture<List<InetSocketAddress>>> submitResolution;
    // Bootstrap timer is lazily initialized on the first poll so its budget represents
    // "time we spend on bootstrap once polling begins" — an idle gap between construction
    // and the first poll should not eat into that budget.
    private Timer timer;
    private CompletableFuture<List<InetSocketAddress>> pendingResolution;
    private volatile long retryMs = -1L;
    private BootstrapResolutionException exception;

    BootstrapResolver(BootstrapConfiguration configuration, Time time, Logger log) {
        // Create executor for async DNS resolution if bootstrap is enabled
        this(configuration, time, log, configuration == BootstrapConfiguration.DISABLED ? null :
            Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("kafka-bootstrap-dns-resolver", true)));
    }

    private BootstrapResolver(BootstrapConfiguration configuration, Time time, Logger log,
                              ExecutorService executor) {
        this(configuration, time, log, executor,
            () -> CompletableFuture.supplyAsync(
                () -> ClientUtils.parseAddresses(configuration.bootstrapServers, configuration.clientDnsLookup), executor));
    }

    BootstrapResolver(BootstrapConfiguration configuration, Time time, Logger log,
                      ExecutorService executor, Supplier<CompletableFuture<List<InetSocketAddress>>> submitResolution) {
        this.configuration = configuration;
        this.time = time;
        this.log = log;
        this.executor = executor;
        this.submitResolution = submitResolution;
        // Kick off the first DNS resolution eagerly so it overlaps with the caller finishing
        // construction. We deliberately don't start the timer here (see field comment);
        // poll() remains the driver — it starts the timer, observes the result, and drives retries.
        // NetworkClient records terminal errors on its metadata updater.
        if (isEnabled())
            pendingResolution = submitResolution.get();
    }

    boolean isEnabled() {
        return configuration != BootstrapConfiguration.DISABLED;
    }

    Optional<Result> poll(long currentTimeMs) {
        if (!isEnabled() || exception != null)
            return Optional.empty();

        if (Thread.interrupted()) {
            cancelResolution();
            throw new InterruptException(new InterruptedException());
        }

        // Start the timer on the first poll so its budget represents "time we spend on
        // bootstrap since polling began" — the caller may have created the client well before
        // its first API call, and we don't want that idle gap to eat into the budget.
        if (timer == null)
            timer = time.timer(configuration.bootstrapResolveTimeoutMs);

        // Check if a pending resolution completed before checking the timeout, so that a
        // result arriving at the same time as the deadline is not incorrectly rejected.
        Optional<Result> result = maybeProcessResolutionResult(currentTimeMs);
        if (result.isPresent())
            return result;

        // Record a timeout failure before possibly triggering a new resolution.
        timer.update(currentTimeMs);
        if (timer.isExpired()) {
            cancelResolution();
            exception = new BootstrapResolutionException("Failed to resolve bootstrap servers after " +
                configuration.bootstrapResolveTimeoutMs + "ms. " +
                "Please check your bootstrap.servers configuration and DNS settings.");
            return Optional.of(Result.failed(exception));
        }

        maybeStartResolution(currentTimeMs);
        return Optional.empty();
    }

    /**
     * Trigger a new async DNS resolution if none is in progress and the retry backoff has elapsed.
     */
    private void maybeStartResolution(long currentTimeMs) {
        if (pendingResolution != null)
            return;

        if (retryMs >= 0 && currentTimeMs < retryMs)
            return;

        retryMs = -1L;
        pendingResolution = submitResolution.get();
    }

    /**
     * Check if a pending bootstrap DNS resolution has completed and process its result.
     */
    private Optional<Result> maybeProcessResolutionResult(long currentTimeMs) {
        if (pendingResolution == null || !pendingResolution.isDone())
            return Optional.empty();

        List<InetSocketAddress> servers = List.of();
        try {
            servers = pendingResolution.getNow(List.of());
        } catch (CompletionException e) {
            log.debug("DNS resolution failed", e);
        }

        pendingResolution = null;
        if (!servers.isEmpty()) {
            log.debug("Bootstrap DNS resolution succeeded, {} servers resolved", servers.size());
            return Optional.of(Result.resolved(servers));
        }

        log.debug("Failed to resolve bootstrap servers, will retry after {}ms. Remaining time: {}ms",
            configuration.retryBackoffMs, timer.remainingMs());
        retryMs = currentTimeMs + configuration.retryBackoffMs;
        return Optional.empty();
    }

    private void cancelResolution() {
        if (pendingResolution != null) {
            pendingResolution.cancel(true);
            pendingResolution = null;
        }
        retryMs = -1L;
    }

    @Override
    public void close() {
        cancelResolution();
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 1, TimeUnit.SECONDS);
    }

    static final class Result {
        final List<InetSocketAddress> addresses;
        final BootstrapResolutionException exception;

        private Result(List<InetSocketAddress> addresses, BootstrapResolutionException exception) {
            this.addresses = addresses;
            this.exception = exception;
        }

        static Result resolved(List<InetSocketAddress> addresses) {
            return new Result(addresses, null);
        }

        static Result failed(BootstrapResolutionException exception) {
            return new Result(null, exception);
        }
    }
}
