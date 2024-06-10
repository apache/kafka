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

package org.apache.kafka.server.network;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Manages a set of per-endpoint futures.
 */
public class EndpointReadyFutures {
    public static class Builder {
        private LogContext logContext = null;
        private final Map<Endpoint, List<EndpointCompletionStage>> endpointStages = new HashMap<>();
        private final List<EndpointCompletionStage> stages = new ArrayList<>();

        /**
         * Add a readiness future that will block all endpoints.
         *
         * @param name          The future name.
         * @param future        The future object.
         *
         * @return              This builder object.
         */
        public Builder addReadinessFuture(
            String name,
            CompletableFuture<?> future
        ) {
            stages.add(new EndpointCompletionStage(name, future));
            return this;
        }

        /**
         * Add readiness futures for individual endpoints.
         *
         * @param name          The future name.
         * @param newFutures    A map from endpoints to futures.
         *
         * @return              This builder object.
         */
        public Builder addReadinessFutures(
            String name,
            Map<Endpoint, ? extends CompletionStage<?>> newFutures
        ) {
            newFutures.forEach((endpoint, future) -> endpointStages.computeIfAbsent(endpoint, __ -> new ArrayList<>()).
                add(new EndpointCompletionStage(name, future)));
            return this;
        }

        /**
         * Build the EndpointReadyFutures object.
         *
         * @param authorizer    The authorizer to use, if any. Will be started.
         * @param info          Server information to be passed to the authorizer.
         *
         * @return              The new futures object.
         */
        public EndpointReadyFutures build(
            Optional<Authorizer> authorizer,
            AuthorizerServerInfo info
        ) {
            if (authorizer.isPresent()) {
                return build(authorizer.get().start(info), info);
            } else {
                return build(Collections.emptyMap(), info);
            }
        }

        EndpointReadyFutures build(
            Map<Endpoint, ? extends CompletionStage<?>> authorizerStartFutures,
            AuthorizerServerInfo info
        ) {
            if (logContext == null) logContext = new LogContext();
            Map<Endpoint, CompletionStage<?>> effectiveStartFutures =
                    new HashMap<>(authorizerStartFutures);
            for (Endpoint endpoint : info.endpoints()) {
                if (!effectiveStartFutures.containsKey(endpoint)) {
                    CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
                    effectiveStartFutures.put(endpoint, completedFuture);
                }
            }
            if (info.endpoints().size() != effectiveStartFutures.size()) {
                List<String> notInInfo = new ArrayList<>();
                for (Endpoint endpoint : effectiveStartFutures.keySet()) {
                    if (!info.endpoints().contains(endpoint)) {
                        notInInfo.add(endpoint.listenerName().orElse("[none]"));
                    }
                }
                throw new RuntimeException("Found authorizer futures that weren't included " +
                        "in AuthorizerServerInfo: " + notInInfo);
            }
            addReadinessFutures("authorizerStart", effectiveStartFutures);
            stages.forEach(stage -> {
                Map<Endpoint, CompletionStage<?>> newReadinessFutures = new HashMap<>();
                info.endpoints().forEach(endpoint -> newReadinessFutures.put(endpoint, stage.future));
                addReadinessFutures(stage.name, newReadinessFutures);
            });
            return new EndpointReadyFutures(logContext,
                    endpointStages);
        }
    }

    static class EndpointCompletionStage {
        final String name;
        final CompletionStage<?> future;

        EndpointCompletionStage(String name, CompletionStage<?> future) {
            this.name = name;
            this.future = future;
        }
    }

    class EndpointReadyFuture {
        final String endpointName;
        final TreeSet<String> incomplete;
        final CompletableFuture<Void> future;

        EndpointReadyFuture(Endpoint endpoint, Collection<String> stageNames) {
            this.endpointName = endpoint.listenerName().orElse("UNNAMED");
            this.incomplete = new TreeSet<>(stageNames);
            this.future = new CompletableFuture<>();
        }

        void completeStage(String stageName) {
            boolean done = false;
            synchronized (EndpointReadyFuture.this) {
                if (incomplete.remove(stageName)) {
                    if (incomplete.isEmpty()) {
                        done = true;
                    } else {
                        log.info("{} completed for endpoint {}. Still waiting for {}.",
                                stageName, endpointName, incomplete);
                    }
                }
            }
            if (done) {
                if (future.complete(null)) {
                    log.info("{} completed for endpoint {}. Endpoint is now READY.",
                            stageName, endpointName);
                }
            }
        }

        void failStage(String what, Throwable exception) {
            if (future.completeExceptionally(exception)) {
                synchronized (EndpointReadyFuture.this) {
                    incomplete.clear();
                }
                log.warn("Endpoint {} will never become ready because we encountered an {} exception",
                        endpointName, what, exception);
            }
        }
    }

    private final Logger log;

    private final Map<Endpoint, CompletableFuture<Void>> futures;

    private EndpointReadyFutures(
        LogContext logContext,
        Map<Endpoint, List<EndpointCompletionStage>> endpointStages
    ) {
        this.log = logContext.logger(EndpointReadyFutures.class);
        Map<Endpoint, CompletableFuture<Void>> newFutures = new HashMap<>();
        endpointStages.forEach((endpoint, stages) -> {
            List<String> stageNames = new ArrayList<>();
            stages.forEach(stage -> stageNames.add(stage.name));
            EndpointReadyFuture readyFuture = new EndpointReadyFuture(endpoint, stageNames);
            newFutures.put(endpoint, readyFuture.future);
            stages.forEach(stage -> stage.future.whenComplete((__, exception) -> {
                if (exception != null) {
                    readyFuture.failStage(stage.name, exception);
                } else {
                    readyFuture.completeStage(stage.name);
                }
            }));
        });
        this.futures = Collections.unmodifiableMap(newFutures);
    }

    public Map<Endpoint, CompletableFuture<Void>> futures() {
        return futures;
    }
}
