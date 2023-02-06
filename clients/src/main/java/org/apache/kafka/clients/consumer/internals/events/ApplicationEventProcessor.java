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

import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.NoopBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.RequestManager;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

public class ApplicationEventProcessor {
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final Map<RequestManager.Type, Optional<RequestManager>> registry;

    public ApplicationEventProcessor(
            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
            final Map<RequestManager.Type, Optional<RequestManager>> requestManagerRegistry) {
        this.backgroundEventQueue = backgroundEventQueue;
        this.registry = requestManagerRegistry;
    }

    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event);
        switch (event.type) {
            case NOOP:
                return process((NoopApplicationEvent) event);
            case COMMIT:
                return process((CommitApplicationEvent) event);
            case POLL:
                return process((PollApplicationEvent) event);
        }
        return false;
    }

    /**
     * Processes {@link NoopApplicationEvent} and equeue a
     * {@link NoopBackgroundEvent}. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a {@link NoopApplicationEvent}
     */
    private boolean process(final NoopApplicationEvent event) {
        return backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    private boolean process(final PollApplicationEvent event) {
        Optional<RequestManager> commitRequestManger = registry.get(RequestManager.Type.COMMIT);
        if (!commitRequestManger.isPresent()) {
            return true;
        }

        CommitRequestManager manager = (CommitRequestManager) commitRequestManger.get();
        manager.clientPoll(event.pollTimeMs);
        return true;
    }

    private boolean process(final CommitApplicationEvent event) {
        Optional<RequestManager> commitRequestManger = registry.get(RequestManager.Type.COMMIT);
        if (!commitRequestManger.isPresent()) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront so we should never get to this block.
            backgroundEventQueue.add(new ErrorBackgroundEvent(
                    new RuntimeException("Unable to commit offset. Most likely because the group.id wasn't set")));
            return false;
        }

        CommitRequestManager manager = (CommitRequestManager) commitRequestManger.get();
        manager.add(event.offsets()).whenComplete((r, e) -> {
            if (e != null) {
                event.future().completeExceptionally(e);
                return;
            }
            event.future().complete(null);
        });
        return true;
    }
}
