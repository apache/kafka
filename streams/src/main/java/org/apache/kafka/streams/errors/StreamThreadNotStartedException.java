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
package org.apache.kafka.streams.errors;

/**
 * Indicates that a task got migrated to another thread.
 * Thus, the task raising this exception can be cleaned up and closed as "zombie".
 */
public class StreamThreadNotStartedException extends RetryableStateStoreException {

    private static final long serialVersionUID = 1L;

    public StreamThreadNotStartedException(final String message) {
        super(message);
    }

    public StreamThreadNotStartedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public StreamThreadNotStartedException(final Throwable throwable) {
        super(throwable);
    }
}
