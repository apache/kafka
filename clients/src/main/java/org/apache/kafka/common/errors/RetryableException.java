/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.errors;

/**
 * A retryable exception is an exception that is safe to retry. To be retryable an exception should be
 * <ol>
 * <li>Transient, there is no point retrying a error due to a non-existant topic or message too large
 * <li>Idempotent, the exception is known to not change any state on the server
 * </ol>
 * A client may choose to retry any request they like, but exceptions extending this class are always safe and sane to
 * retry.
 */
public abstract class RetryableException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }

    public RetryableException() {
    }

}
