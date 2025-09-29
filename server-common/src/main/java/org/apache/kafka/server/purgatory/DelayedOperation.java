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

import org.apache.kafka.server.util.timer.TimerTask;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 * <br/>
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 * <br/>
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 * <br/>
 * Noted that if you add a future delayed operation that calls ReplicaManager.appendRecords() in onComplete()
 * like DelayedJoin, you must be aware that this operation's onExpiration() needs to call actionQueue.tryCompleteAction().
 */
public abstract class DelayedOperation extends TimerTask {

    private final AtomicBoolean completed = new AtomicBoolean(false);

    protected final Lock lock;

    public DelayedOperation(long delayMs, Optional<Lock> lockOpt) {
        this(delayMs, lockOpt.orElse(new ReentrantLock()));
    }

    public DelayedOperation(long delayMs) {
        this(delayMs, new ReentrantLock());
    }

    public DelayedOperation(long delayMs, Lock lock) {
        super(delayMs);
        this.lock = lock;
    }

    /*
     * Force completing the delayed operation, if not already completed.
     * This function can be triggered when
     *
     * 1. The operation has been verified to be completable inside tryComplete()
     * 2. The operation has expired and hence needs to be completed right now
     *
     * Return true iff the operation is completed by the caller: note that
     * concurrent threads can try to complete the same operation, but only
     * the first thread will succeed in completing the operation and return
     * true, others will still return false
     */
    public boolean forceComplete() {
        if (completed.compareAndSet(false, true)) {
            // cancel the timeout timer
            cancel();
            onComplete();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the delayed operation is already completed
     */
    public boolean isCompleted() {
        return completed.get();
    }

    /**
     * Call-back to execute when a delayed operation gets expired and hence forced to complete.
     */
    public abstract void onExpiration();

    /**
     * Process for completing an operation; This function needs to be defined
     * in subclasses and will be called exactly once in forceComplete()
     */
    public abstract void onComplete();

    /**
     * Try to complete the delayed operation by first checking if the operation
     * can be completed by now. If yes execute the completion logic by calling
     * forceComplete() and return true iff forceComplete returns true; otherwise return false
     * <br/>
     * This function needs to be defined in subclasses
     */
    public abstract boolean tryComplete();

    /**
     * Thread-safe variant of tryComplete() and call extra function if first tryComplete returns false
     * @param action else function to be executed after first tryComplete returns false
     * @return result of tryComplete
     */
    boolean safeTryCompleteOrElse(Action action) {
        lock.lock();
        try {
            if (tryComplete()) return true;
            else {
                action.apply();
                // last completion check
                return tryComplete();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Thread-safe variant of tryComplete()
     */
    boolean safeTryComplete() {
        lock.lock();
        try {
            if (isCompleted()) return false;
            else return tryComplete();
        } finally {
            lock.unlock();
        }
    }

    /**
     * run() method defines a task that is executed on timeout
     */
    @Override
    public void run() {
        if (forceComplete())
            onExpiration();
    }

    @FunctionalInterface
    public interface Action {
        void apply();
    }
}
