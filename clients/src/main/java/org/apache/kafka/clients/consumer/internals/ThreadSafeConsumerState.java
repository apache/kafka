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
package org.apache.kafka.clients.consumer.internals;

/**
 * This class stores shared state needed by both the application thread and the background thread to avoid costly
 * inter-thread communication, where possible. This class compromises on the ideal of keeping state only in the
 * background thread. However, this class (and its subclasses) only relies on classes which are designed to be
 * thread-safe, thus they can be used in both the application and background threads.
 *
 * <p/>
 *
 * The following thread-safe classes are used by this class:
 *
 * <ul>
 *     <li>{@link ThreadSafeExceptionReference}</li>
 * </ul>
 *
 * <p/>
 *
 * In general, callers from the application thread should not mutate any of the state contained within this class.
 * It should be considered as <em>read-only</em>, and only the background thread should mutate the state.
 */
public class ThreadSafeConsumerState {

    protected final ThreadSafeExceptionReference metadataError;

    public ThreadSafeConsumerState() {
        this.metadataError = new ThreadSafeExceptionReference();
    }

    public ThreadSafeExceptionReference metadataError() {
        return metadataError;
    }
}
