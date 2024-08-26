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

import java.util.concurrent.BlockingQueue;

/**
 * An {@code EventProcessor} is the means by which events are <em>processed</em>, the meaning of which is left
 * intentionally loose. This is in large part to keep the {@code EventProcessor} focused on what it means to process
 * the events, and <em>not</em> linking itself too closely with the rest of the surrounding application.
 *
 * <p/>
 *
 * The {@code EventProcessor} is envisaged as a stateless service that acts as a conduit, receiving an event and
 * dispatching to another block of code to process. The semantic meaning of each event is different, so the
 * {@code EventProcessor} will need to interact with other parts of the system that maintain state. The
 * implementation should not be concerned with the mechanism by which an event arrived for processing. While the
 * events are shuffled around the consumer subsystem by means of {@link BlockingQueue shared queues}, it should
 * be considered an anti-pattern to need to know how it arrived or what happens after its is processed.
 */
public interface EventProcessor<T> {

    /**
     * Process an event that is received.
     */
    void process(T event);
}
