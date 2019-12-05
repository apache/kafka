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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.KafkaException;

/**
 * Denotes an exception that is recoverable by re-creating the client (ie, the client is no longer in a valid state),
 * as opposed to retriable (the failure was transient, so the same client can be used again later),
 * or fatal (the request was actually invalid, so retrying or recovering would not help)
 *
 * This class also serves the dual purpose of capturing the stack trace as early as possible,
 * at the site of the Producer call, since the exeptions that cause this don't record stack traces.
 */
public class RecoverableClientException extends KafkaException {
    public RecoverableClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
