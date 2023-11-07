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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

/**
 * An event handler that receives {@link ApplicationEvent application events} from the application thread which
 * are then readable from the {@link ApplicationEventProcessor} in the {@link ConsumerNetworkThread network thread}.
 */
class InternalApplicationEventHandler extends ApplicationEventHandler {

    private final Logger log;
    private final ConsumerNetworkThread networkThread;
    private final IdempotentCloser closer = new IdempotentCloser();

    InternalApplicationEventHandler(final LogContext logContext,
                                    final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                    final ConsumerNetworkThread networkThread) {
        super(logContext, applicationEventQueue, networkThread::wakeup);
        this.log = logContext.logger(InternalApplicationEventHandler.class);
        this.networkThread = networkThread;
        this.networkThread.start();
    }

    @Override
    public void close(final Duration timeout) {
        closer.close(
                () -> {
                    Utils.closeQuietly(() -> networkThread.close(timeout), "consumer network thread");
                    super.close(timeout);
                },
                () -> log.warn("The application event handler was already closed")
        );
    }
}
