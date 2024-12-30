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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DelayedOperationPurgatory<T extends DelayedOperation> {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedOperationPurgatory.class);
    private static final int SHARDS = 512; // Shard the watcher list to reduce lock contention

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "DelayedOperationPurgatory");
    private final Map<String, String> metricsTags;
    private final List<WatcherList> watcherLists;
    // the number of estimated total operations in the purgatory
    private final AtomicInteger estimatedTotalOperations = new AtomicInteger(0);
    /* background thread expiring operations that have timed out */
    private final ExpiredOperationReaper expirationReaper = new ExpiredOperationReaper();
    private final String purgatoryName;
    private final Timer timeoutTimer;
    private final int brokerId;
    private final int purgeInterval;
    private final boolean reaperEnabled;
    private final boolean timerEnabled;

    public DelayedOperationPurgatory(String purgatoryName, Timer timer, int brokerId, boolean reaperEnabled) {
        this(purgatoryName, timer, brokerId, 1000, reaperEnabled, true);
    }

    public DelayedOperationPurgatory(String purgatoryName, int brokerId) {
        this(purgatoryName, brokerId, 1000);
    }

    public DelayedOperationPurgatory(String purgatoryName, int brokerId, int purgeInterval) {
        this(purgatoryName, new SystemTimer(purgatoryName), brokerId, purgeInterval, true, true);
    }

    /**
     * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
     */
    @SuppressWarnings("this-escape")
    public DelayedOperationPurgatory(String purgatoryName,
                                     Timer timeoutTimer,
                                     int brokerId,
                                     int purgeInterval,
                                     boolean reaperEnabled,
                                     boolean timerEnabled) {
        this.purgatoryName = purgatoryName;
        this.timeoutTimer = timeoutTimer;
        this.brokerId = brokerId;
        this.purgeInterval = purgeInterval;
        this.reaperEnabled = reaperEnabled;
        this.timerEnabled = timerEnabled;

        watcherLists = new ArrayList<>(SHARDS);
        for (int i = 0; i < SHARDS; i++) {
            watcherLists.add(new WatcherList());
        }
        metricsTags = Collections.singletonMap("delayedOperation", purgatoryName);
        metricsGroup.newGauge("PurgatorySize", this::watched, metricsTags);
        metricsGroup.newGauge("NumDelayedOperations", this::numDelayed, metricsTags);
        if (reaperEnabled) {
            expirationReaper.start();
        }
    }

    private WatcherList watcherList(DelayedOperationKey key) {
        return watcherLists.get(Math.abs(key.hashCode() % watcherLists.size()));
    }

    /**
     * Check if the operation can be completed, if not watch it based on the given watch keys
     * <br/>
     * Note that a delayed operation can be watched on multiple keys. It is possible that
     * an operation is completed after it has been added to the watch list for some, but
     * not all the keys. In this case, the operation is considered completed and won't
     * be added to the watch list of the remaining keys. The expiration reaper thread will
     * remove this operation from any watcher list in which the operation exists.
     *
     * @param operation the delayed operation to be checked
     * @param watchKeys keys for bookkeeping the operation
     * @return true iff the delayed operations can be completed by the caller
     */
    public <K extends DelayedOperationKey> boolean tryCompleteElseWatch(T operation, List<K> watchKeys) {
        if (watchKeys.isEmpty()) {
            throw new IllegalArgumentException("The watch key list can't be empty");
        }

        // The cost of tryComplete() is typically proportional to the number of keys. Calling tryComplete() for each key is
        // going to be expensive if there are many keys. Instead, we do the check in the following way through safeTryCompleteOrElse().
        // If the operation is not completed, we just add the operation to all keys. Then we call tryComplete() again. At
        // this time, if the operation is still not completed, we are guaranteed that it won't miss any future triggering
        // event since the operation is already on the watcher list for all keys.
        //
        // ==============[story about lock]==============
        // Through safeTryCompleteOrElse(), we hold the operation's lock while adding the operation to watch list and doing
        // the tryComplete() check. This is to avoid a potential deadlock between the callers to tryCompleteElseWatch() and
        // checkAndComplete(). For example, the following deadlock can happen if the lock is only held for the final tryComplete()
        // 1) thread_a holds readlock of stateLock from TransactionStateManager
        // 2) thread_a is executing tryCompleteElseWatch()
        // 3) thread_a adds op to watch list
        // 4) thread_b requires writelock of stateLock from TransactionStateManager (blocked by thread_a)
        // 5) thread_c calls checkAndComplete() and holds lock of op
        // 6) thread_c is waiting readlock of stateLock to complete op (blocked by thread_b)
        // 7) thread_a is waiting lock of op to call the final tryComplete() (blocked by thread_c)
        //
        // Note that even with the current approach, deadlocks could still be introduced. For example,
        // 1) thread_a calls tryCompleteElseWatch() and gets lock of op
        // 2) thread_a adds op to watch list
        // 3) thread_a calls op#tryComplete and tries to require lock_b
        // 4) thread_b holds lock_b and calls checkAndComplete()
        // 5) thread_b sees op from watch list
        // 6) thread_b needs lock of op
        // To avoid the above scenario, we recommend DelayedOperationPurgatory.checkAndComplete() be called without holding
        // any exclusive lock. Since DelayedOperationPurgatory.checkAndComplete() completes delayed operations asynchronously,
        // holding an exclusive lock to make the call is often unnecessary.
        if (operation.safeTryCompleteOrElse(() -> {
            watchKeys.forEach(key -> {
                if (!operation.isCompleted())
                    watchForOperation(key, operation);
            });
            if (!watchKeys.isEmpty())
                estimatedTotalOperations.incrementAndGet();
        })) {
            return true;
        }

        // if it cannot be completed by now and hence is watched, add to the timeout queue also
        if (!operation.isCompleted()) {
            if (timerEnabled)
                timeoutTimer.add(operation);
            if (operation.isCompleted()) {
                // cancel the timer task
                operation.cancel();
            }
        }
        return false;
    }

    /**
     * Check if some delayed operations can be completed with the given watch key,
     * and if yes complete them.
     *
     * @return the number of completed operations during this process
     */
    public <K extends DelayedOperationKey> int checkAndComplete(K key) {
        WatcherList wl = watcherList(key);
        Watchers watchers;
        wl.watchersLock.lock();
        try {
            watchers = wl.watchersByKey.get(key);
        } finally {
            wl.watchersLock.unlock();
        }
        int numCompleted = watchers == null ? 0 : watchers.tryCompleteWatched();

        if (numCompleted > 0) {
            LOG.debug("Request key {} unblocked {} {} operations", key, numCompleted, purgatoryName);
        }
        return numCompleted;
    }

    /**
     * Return the total size of watch lists the purgatory. Since an operation may be watched
     * on multiple lists, and some of its watched entries may still be in the watch lists
     * even when it has been completed, this number may be larger than the number of real operations watched
     */
    public int watched() {
        int sum = 0;
        for (WatcherList watcherList : watcherLists) {
            sum += watcherList.allWatchers().stream().mapToInt(Watchers::countWatched).sum();
        }
        return sum;
    }

    /**
     * Return the number of delayed operations in the expiry queue
     */
    public int numDelayed() {
        return timeoutTimer.size();
    }

    /**
     * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
     */
    public List<T> cancelForKey(DelayedOperationKey key) {
        WatcherList wl = watcherList(key);
        wl.watchersLock.lock();
        try {
            Watchers watchers = wl.watchersByKey.remove(key);
            if (watchers != null)
                return watchers.cancel();
            else
                return Collections.emptyList();
        } finally {
            wl.watchersLock.unlock();
        }
    }

    /*
     * Watch the operation
     */
    private void watchForOperation(DelayedOperationKey key, T operation) {
        WatcherList wl = watcherList(key);
        wl.watchersLock.lock();
        try {
            Watchers watcher = wl.watchersByKey.computeIfAbsent(key, Watchers::new);
            watcher.watch(operation);
        } finally {
            wl.watchersLock.unlock();
        }
    }

    /*
     * Remove the key from watcher lists if its list is empty
     */
    private void removeKeyIfEmpty(DelayedOperationKey key, Watchers watchers) {
        WatcherList wl = watcherList(key);
        wl.watchersLock.lock();
        try {
            // if the current key is no longer correlated to the watchers to remove, skip
            if (wl.watchersByKey.get(key) != watchers)
                return;

            if (watchers != null && watchers.isEmpty()) {
                wl.watchersByKey.remove(key);
            }
        } finally {
            wl.watchersLock.unlock();
        }
    }

    /**
     * Shutdown the expiration reaper thread
     */
    public void shutdown() throws Exception {
        if (reaperEnabled) {
            expirationReaper.initiateShutdown();
            // improve shutdown time by waking up any ShutdownableThread blocked on poll by sending a no-op
            timeoutTimer.add(new TimerTask(0) {
                @Override
                public void run() {}
            });
            expirationReaper.awaitShutdown();
        }
        timeoutTimer.close();
        metricsGroup.removeMetric("PurgatorySize", metricsTags);
        metricsGroup.removeMetric("NumDelayedOperations", metricsTags);
    }

    /**
     * A list of operation watching keys
     */
    private class WatcherList {
        private final ConcurrentHashMap<DelayedOperationKey, Watchers> watchersByKey = new ConcurrentHashMap<>();

        private final ReentrantLock watchersLock = new ReentrantLock();

        /*
         * Return all the current watcher lists,
         * note that the returned watchers may be removed from the list by other threads
         */
        Collection<Watchers> allWatchers() {
            return watchersByKey.values();
        }
    }

    /**
     * A linked list of watched delayed operations based on some key
     */
    private class Watchers {

        private final ConcurrentLinkedQueue<T> operations = new ConcurrentLinkedQueue<>();

        private final DelayedOperationKey key;
        Watchers(DelayedOperationKey key) {
            this.key = key;
        }

        // count the current number of watched operations. This is O(n), so use isEmpty() if possible
        int countWatched() {
            return operations.size();
        }

        boolean isEmpty() {
            return operations.isEmpty();
        }

        // add the element to watch
        void watch(T t) {
            operations.add(t);
        }

        // traverse the list and try to complete some watched elements
        int tryCompleteWatched() {
            int completed = 0;

            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    // another thread has completed this operation, just remove it
                    iter.remove();
                } else if (curr.safeTryComplete()) {
                    iter.remove();
                    completed += 1;
                }
            }

            if (operations.isEmpty())
                removeKeyIfEmpty(key, this);

            return completed;
        }

        List<T> cancel() {
            Iterator<T> iter = operations.iterator();
            List<T> cancelled = new ArrayList<>();
            while (iter.hasNext()) {
                T curr = iter.next();
                curr.cancel();
                iter.remove();
                cancelled.add(curr);
            }
            return cancelled;
        }

        // traverse the list and purge elements that are already completed by others
        int purgeCompleted() {
            int purged = 0;

            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    iter.remove();
                    purged += 1;
                }
            }

            if (operations.isEmpty())
                removeKeyIfEmpty(key, this);

            return purged;
        }
    }

    private void advanceClock(long timeoutMs) throws InterruptedException {
        timeoutTimer.advanceClock(timeoutMs);

        // Trigger a purge if the number of completed but still being watched operations is larger than
        // the purge threshold. That number is computed by the difference btw the estimated total number of
        // operations and the number of pending delayed operations.
        if (estimatedTotalOperations.get() - numDelayed() > purgeInterval) {
            // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
            // clean up watchers. Note that, if more operations are completed during the cleanup, we may end up with
            // a little overestimated total number of operations.
            estimatedTotalOperations.getAndSet(numDelayed());
            LOG.debug("Begin purging watch lists");
            int purged = 0;
            for (WatcherList watcherList : watcherLists) {
                purged += watcherList.allWatchers().stream().mapToInt(Watchers::purgeCompleted).sum();
            }
            LOG.debug("Purged {} elements from watch lists.", purged);
        }
    }

    /**
     * A background reaper to expire delayed operations that have timed out
     */
    private class ExpiredOperationReaper extends ShutdownableThread {
        ExpiredOperationReaper() {
            super("ExpirationReaper-" + brokerId + "-" + purgatoryName, false);
        }

        @Override
        public void doWork() {
            try {
                advanceClock(200L);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }
}
