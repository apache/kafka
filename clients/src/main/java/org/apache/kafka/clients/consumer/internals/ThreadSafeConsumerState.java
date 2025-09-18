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
 * This class stores shared state needed by both the application thread ({@link AsyncKafkaConsumer}) and the
 * network thread ({@link ConsumerNetworkThread}) to avoid costly inter-thread communication, where possible.
 * This class compromises on the ideal of keeping state only in the network thread. However, this class only
 * relies on classes which are designed to be thread-safe, thus they can be used in both the application
 * and network threads.
 *
 * <p/>
 *
 * The following thread-safe classes are used by this class:
 *
 * <ul>
 *     <li>{@link ThreadSafeAutoCommitState}</li>
 *     <li>{@link ThreadSafeReconciliationState}</li>
 * </ul>
 *
 * <p/>
 *
 * In general, callers from the application thread should not mutate any of the state contained within this class.
 * It should be considered as <em>read-only</em>, and only the network thread should mutate the state.
 */
public abstract class ThreadSafeConsumerState {

    protected final ThreadSafeReconciliationState reconciliationState;

    protected ThreadSafeConsumerState() {
        this.reconciliationState = new ThreadSafeReconciliationState();
    }

    public abstract ThreadSafeAutoCommitState autoCommitState();

    public ThreadSafeReconciliationState reconciliationState() {
        return reconciliationState;
    }
}
