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

import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;


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
}
