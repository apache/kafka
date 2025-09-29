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

package org.apache.kafka.controller.errors;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.mutable.BoundedListTooLongException;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;


public final class EventHandlerExceptionInfo {
    /**
     * True if this exception should be treated as a fault, and tracked via the metadata errors
     * metric.
     */
    private final boolean isFault;

    /**
     * True if this exception should cause a controller failover.
     * All faults cause failover
     */
    private final boolean causesFailover;

    /**
     * The internal exception.
     */
    private final Throwable internalException;

    /**
     * The exception to present to RPC callers, or Optional.empty if the internal exception should
     * be presented directly.
     */
    private final Optional<Throwable> externalException;

    /**
     * Create an EventHandlerExceptionInfo object from an internal exception.
     *
     * @param internal                  The internal exception.
     * @param latestControllerSupplier  A function we can call to obtain the latest leader id.
     *
     * @return                          The new immutable info object.
     */
    public static EventHandlerExceptionInfo fromInternal(
        Throwable internal,
        Supplier<OptionalInt> latestControllerSupplier
    ) {
        if (internal instanceof ApiException) {
            // This exception is a standard API error response from the controller, which can pass
            // through without modification.
            return new EventHandlerExceptionInfo(false, false, internal);
        } else if (internal instanceof NotLeaderException) {
            // The controller has lost leadership.
            return new EventHandlerExceptionInfo(false, true, internal,
                ControllerExceptions.newWrongControllerException(latestControllerSupplier.get()));
        } else if (internal instanceof RejectedExecutionException) {
            // The controller event queue is shutting down.
            return new EventHandlerExceptionInfo(false, false, internal,
                new TimeoutException("The controller is shutting down.", internal));
        } else if (internal instanceof BoundedListTooLongException) {
            // The operation could not be performed because it would have created an overly large
            // batch.
            return new EventHandlerExceptionInfo(false, false, internal,
                new PolicyViolationException("Unable to perform excessively large batch " +
                    "operation."));
        } else if (internal instanceof PeriodicControlTaskException) {
            // This exception is a periodic task which failed.
            return new EventHandlerExceptionInfo(true, false, internal);
        } else if (internal instanceof InterruptedException) {
            // The controller event queue has been interrupted. This normally only happens during
            // a JUnit test that has hung. The test framework sometimes sends an InterruptException
            // to all threads to try to get them to shut down. This isn't the correct way to shut
            // the test, but it may happen if something hung.
            return new EventHandlerExceptionInfo(true, true, internal,
                new UnknownServerException("The controller was interrupted."));
        } else {
            // This is the catch-all case for things that aren't supposed to happen. Null pointer
            // exceptions, illegal argument exceptions, etc. They get translated into an
            // UnknownServerException and a controller failover.
            return new EventHandlerExceptionInfo(true, true, internal,
                new UnknownServerException(internal));
        }
    }

    /**
     * Returns true if the class and message fields match for two exceptions. Handles nulls.
     */
    static boolean exceptionClassesAndMessagesMatch(Throwable a, Throwable b) {
        if (a == null) return b == null;
        if (b == null) return false;
        if (!a.getClass().equals(b.getClass())) return false;
        return Objects.equals(a.getMessage(), b.getMessage());
    }

    EventHandlerExceptionInfo(
        boolean isFault,
        boolean causesFailover,
        Throwable internalException
    ) {
        this.isFault = isFault;
        this.causesFailover = causesFailover;
        this.internalException = internalException;
        this.externalException = Optional.empty();
    }

    EventHandlerExceptionInfo(
        boolean isFault,
        boolean causesFailover,
        Throwable internalException,
        Throwable externalException
    ) {
        this.isFault = isFault;
        this.causesFailover = causesFailover;
        this.internalException = internalException;
        this.externalException = Optional.of(externalException);
    }

    public boolean isFault() {
        return isFault;
    }

    public boolean causesFailover() {
        return causesFailover;
    }

    public Throwable effectiveExternalException() {
        return externalException.orElse(internalException);
    }

    public boolean isTimeoutException() {
        return internalException instanceof TimeoutException;
    }

    public String failureMessage(
        int epoch,
        OptionalLong deltaUs,
        boolean isActiveController,
        long lastCommittedOffset
    ) {
        StringBuilder bld = new StringBuilder();
        if (deltaUs.isPresent()) {
            bld.append("event failed with ");
        } else {
            bld.append("event unable to start processing because of ");
        }
        bld.append(internalException.getClass().getSimpleName());
        if (externalException.isPresent()) {
            bld.append(" (treated as ").
                append(externalException.get().getClass().getSimpleName()).append(")");
        }
        if (causesFailover()) {
            bld.append(" at epoch ").append(epoch);
        }
        if (deltaUs.isPresent()) {
            bld.append(" in ").append(deltaUs.getAsLong()).append(" microseconds");
        }
        if (causesFailover()) {
            if (isActiveController) {
                bld.append(". Renouncing leadership and reverting to the last committed offset ");
                bld.append(lastCommittedOffset);
            } else {
                bld.append(". The controller is already in standby mode");
            }
        }
        bld.append(".");
        if (!isFault && internalException.getMessage() != null) {
            bld.append(" Exception message: ");
            bld.append(internalException.getMessage());
        }
        return bld.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isFault,
                causesFailover,
                internalException.getClass().getCanonicalName(),
                internalException.getMessage(),
                externalException.orElse(internalException).getClass().getCanonicalName(),
                externalException.orElse(internalException).getMessage());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(EventHandlerExceptionInfo.class))) return false;
        EventHandlerExceptionInfo other = (EventHandlerExceptionInfo) o;
        return isFault == other.isFault &&
                causesFailover == other.causesFailover &&
                exceptionClassesAndMessagesMatch(internalException, other.internalException) &&
                exceptionClassesAndMessagesMatch(externalException.orElse(null),
                        other.externalException.orElse(null));
    }

    @Override
    public String toString() {
        return "EventHandlerExceptionInfo" +
            "(isFault=" + isFault +
            ", causesFailover=" + causesFailover +
            ", internalException.class=" + internalException.getClass().getCanonicalName() +
            ", externalException.class=" + (externalException.isPresent() ?
                externalException.get().getClass().getCanonicalName() : "(none)") +
            ")";
    }
}
