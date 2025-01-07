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

package org.apache.kafka.server.fault;


/**
 * An exception thrown by a fault handler.
 */
public final class FaultHandlerException extends RuntimeException {

    public FaultHandlerException(String failureMessage, Throwable cause) {
        super(failureMessage, cause);
        // If a cause exception was provided, set our the stack trace its stack trace. This is
        // useful in junit tests where a limited number of stack frames are printed, and usually
        // the stack frames of cause exceptions get trimmed.
        if (cause != null) {
            setStackTrace(cause.getStackTrace());
        }
    }

    public FaultHandlerException(String failureMessage) {
        this(failureMessage, null);
    }
}
