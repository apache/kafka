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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;

/**
 * This interface is used for events that need to be notified when the
 * {@link NetworkClientDelegate#getAndClearMetadataError()} has an error.
 */
public interface MetadataErrorNotifiableEvent {

    /**
     * The background thread detects metadata errors on every call to {@link NetworkClientDelegate#poll(long, long)}.
     * {@link NetworkClientDelegate} calls {@link Metadata#maybeThrowAnyException()} and stores the result.
     * The presence of a metadata error is checked in the {@link ConsumerNetworkThread}'s loop by calling
     * {@link NetworkClientDelegate#getAndClearMetadataError()}. There are two places in the loop in which the
     * metadata error is checked:
     *
     * <ul>
     *     <li>
     *         At the very top of the {@link ConsumerNetworkThread}'s loop, the {@link ApplicationEventHandler}'s
     *         queue is drained. Before processing each event via
     *         {@link ApplicationEventProcessor#process(ApplicationEvent)}, if a metadata error occurred, this method
     *         will be invoked on the event if it implements this interface.
     *         <p/>
     *         <em>Note</em>: for an event on which this method is invoked, it will <em>not</em> be passed to the
     *         {@link ApplicationEventProcessor#process(ApplicationEvent)} method.
     *     </li>
     *     <li>
     *         At the very bottom of the {@link ConsumerNetworkThread}'s loop, the {@link CompletableEventReaper}
     *         is executed and any outstanding event is returned. If a metadata error occurred, this method
     *         will be invoked on all unexpired events if it implements this interface.
     *     </li>
     * </ul>
     *
     * @param metadataError Error that originally came from {@link Metadata#maybeThrowAnyException()}
     */
    void onMetadataError(Exception metadataError);
}
