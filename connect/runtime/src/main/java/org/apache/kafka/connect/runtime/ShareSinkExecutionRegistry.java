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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class ShareSinkExecutionRegistry {
    private static final Runnable NO_RETIREMENT = () -> { };
    private final Map<ConnectorTaskId, Reservation> reservations = new HashMap<>();
    private long lastIncarnation;
    private int orphans;

    synchronized Reservation reserve(ConnectorTaskId id) {
        Objects.requireNonNull(id, "id");
        if (orphans != 0) {
            throw new ConnectException("Cannot start share task " + id + ": this worker has an orphaned share execution");
        }
        if (reservations.containsKey(id)) {
            throw new ConnectException("Share task execution is still reserved in this worker: " + id);
        }
        if (lastIncarnation == Long.MAX_VALUE) {
            throw new ConnectException("Share task incarnation space is exhausted in this worker");
        }
        Reservation reservation = new Reservation(this, id, ++lastIncarnation);
        reservations.put(id, reservation);
        return reservation;
    }

    synchronized boolean hasOrphans() {
        return orphans != 0;
    }

    synchronized boolean contains(ConnectorTaskId id) {
        return reservations.containsKey(id);
    }

    synchronized int size() {
        return reservations.size();
    }

    private synchronized void markOrphan(Reservation reservation) {
        if (reservations.get(reservation.id) == reservation && !reservation.orphaned) {
            reservation.orphaned = true;
            orphans++;
        }
    }

    private synchronized void onRetirement(Reservation reservation, Runnable action) {
        Objects.requireNonNull(action, "action");
        if (reservations.get(reservation.id) != reservation || reservation.retiring) {
            throw new IllegalStateException("Share task execution is already retiring or retired: " + reservation.id);
        }
        if (reservation.retirement != null) {
            throw new IllegalStateException("Share task retirement callback is already registered: " + reservation.id);
        }
        reservation.retirement = action;
    }

    private void exited(Reservation reservation, boolean plugin, boolean cleanupComplete) {
        Runnable retirement = null;
        synchronized (this) {
            if (reservations.get(reservation.id) != reservation) {
                return;
            }
            ExitStatus status = cleanupComplete ? ExitStatus.CLEAN : ExitStatus.UNCLEAN;
            if (plugin) {
                if (reservation.pluginExit != ExitStatus.RUNNING) {
                    return;
                }
                reservation.pluginExit = status;
            } else {
                if (reservation.controllerExit != ExitStatus.RUNNING) {
                    return;
                }
                reservation.controllerExit = status;
            }
            if (!cleanupComplete) {
                markOrphan(reservation);
            }
            if (reservation.pluginExit == ExitStatus.CLEAN && reservation.controllerExit == ExitStatus.CLEAN) {
                reservation.retiring = true;
                retirement = reservation.retirement == null ? NO_RETIREMENT : reservation.retirement;
            }
        }
        if (retirement != null) {
            retire(reservation, retirement);
        }
    }

    private void retire(Reservation reservation, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | Error failure) {
            markOrphan(reservation);
            throw failure;
        }
        synchronized (this) {
            if (reservations.get(reservation.id) != reservation) {
                return;
            }
            reservations.remove(reservation.id);
            if (reservation.orphaned) {
                orphans--;
            }
        }
        reservation.released.complete(null);
    }

    private enum ExitStatus {
        RUNNING,
        CLEAN,
        UNCLEAN
    }

    static final class Reservation {
        private final ShareSinkExecutionRegistry registry;
        private final ConnectorTaskId id;
        private final long incarnation;
        private final CompletableFuture<Void> released = new CompletableFuture<>();
        private final CompletionStage<Void> releaseStage = released.minimalCompletionStage();
        private ExitStatus pluginExit = ExitStatus.RUNNING;
        private ExitStatus controllerExit = ExitStatus.RUNNING;
        private boolean orphaned;
        private boolean retiring;
        private Runnable retirement;

        private Reservation(ShareSinkExecutionRegistry registry, ConnectorTaskId id, long incarnation) {
            this.registry = registry;
            this.id = id;
            this.incarnation = incarnation;
        }

        ConnectorTaskId id() {
            return id;
        }

        long incarnation() {
            return incarnation;
        }

        void markOrphan() {
            registry.markOrphan(this);
        }

        void onRetirement(Runnable action) {
            registry.onRetirement(this, action);
        }

        void pluginExited(boolean cleanupComplete) {
            registry.exited(this, true, cleanupComplete);
        }

        void controllerExited(boolean cleanupComplete) {
            registry.exited(this, false, cleanupComplete);
        }

        CompletionStage<Void> released() {
            return releaseStage;
        }
    }
}
