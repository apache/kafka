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
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.mutable.BoundedListTooLongException;

import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;


public class ControllerExceptions {
    /**
     * Check if an exception is a normal timeout exception.
     *
     * @param exception     The exception to check.
     * @return              True if the exception is a timeout exception.
     */
    public static boolean isTimeoutException(Throwable exception) {
        if (exception == null) return false;
        if (exception instanceof ExecutionException) {
            exception = exception.getCause();
            if (exception == null) return false;
        }
        if (!(exception instanceof TimeoutException)) return false;
        return true;
    }

    /**
     * Check if an exception is a NotController exception.
     *
     * @param exception     The exception to check.
     * @return              True if the exception is a NotController exception.
     */
    public static boolean isNotControllerException(Throwable exception) {
        if (exception == null) return false;
        if (exception instanceof ExecutionException) {
            exception = exception.getCause();
            if (exception == null) return false;
        }
        if (!(exception instanceof NotControllerException)) return false;
        return true;
    }

    /**
     * Create a new exception indicating that the controller is in pre-migration mode, so the
     * operation cannot be completed.
     *
     * @param controllerId      The current controller.
     * @return                  The new NotControllerException.
     */
    public static NotControllerException newPreMigrationException(OptionalInt controllerId) {
        if (controllerId.isPresent()) {
            return new NotControllerException("The controller is in pre-migration mode.");
        } else {
            return new NotControllerException("No controller appears to be active.");
        }
    }

    /**
     * Create a new exception indicating that current node is not the controller.
     *
     * @param controllerId      The current controller.
     * @return                  The new NotControllerException.
     */
    public static NotControllerException newWrongControllerException(OptionalInt controllerId) {
        if (controllerId.isPresent()) {
            return new NotControllerException("The active controller appears to be node " +
                    controllerId.getAsInt() + ".");
        } else {
            return new NotControllerException("No controller appears to be active.");
        }
    }

    /**
     * Determine if an exception is expected. Unexpected exceptions trigger controller failovers
     * when they are raised.
     *
     * @param exception     The exception.
     * @return              True if the exception is expected.
     */
    public static boolean isExpected(Throwable exception) {
        if (exception instanceof ApiException) {
            // ApiExceptions indicate errors that should be returned to the user.
            return true;
        } else if (exception instanceof NotLeaderException) {
            // NotLeaderException is thrown if we try to append records, but are not the leader.
            return true;
        } else if (exception instanceof RejectedExecutionException) {
            // This can happen when the controller is shutting down.
            return true;
        } else if (exception instanceof BoundedListTooLongException) {
            // This can happen if we tried to create too many records.
            return true;
        } else if (exception instanceof InterruptedException) {
            // Interrupted exceptions are not expected. They might happen during junit tests if
            // the test gets stuck and must be terminated by sending IE to all the threads.
            return false;
        }
        // Other exceptions are unexpected.
        return false;
    }

    /**
     * Translate an internal controller exception to its external equivalent.
     *
     * @param exception     The internal exception.
     * @return              Its external equivalent.
     */
    public static Throwable toExternalException(
        Throwable exception,
        Supplier<OptionalInt> latestControllerSupplier
    ) {
        if (exception instanceof ApiException) {
            return exception;
        } else if (exception instanceof NotLeaderException) {
            return newWrongControllerException(latestControllerSupplier.get());
        } else if (exception instanceof RejectedExecutionException) {
            return new TimeoutException("The controller is shutting down.", exception);
        } else if (exception instanceof BoundedListTooLongException) {
            return new PolicyViolationException("Unable to perform excessively large batch " +
                    "operation.");
        } else if (exception instanceof InterruptedException) {
            return new UnknownServerException("The controller was interrupted.");
        }
        return new UnknownServerException(exception);
    }
}
