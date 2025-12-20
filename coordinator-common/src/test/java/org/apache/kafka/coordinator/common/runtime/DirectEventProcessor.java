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
package org.apache.kafka.coordinator.common.runtime;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.RejectedExecutionException;

/**
 * A CoordinatorEventProcessor that directly executes the operations. This is
 * useful in unit tests where execution in threads is not required.
 */
public class DirectEventProcessor implements CoordinatorEventProcessor {
    private final Deque<CoordinatorEvent> queue;
    private boolean inEvent;

    public DirectEventProcessor() {
        this.queue = new LinkedList<>();
        this.inEvent = false;
    }

    @Override
    public void enqueueLast(CoordinatorEvent event) throws RejectedExecutionException {
        queue.addLast(event);
        processQueue();
    }

    @Override
    public void enqueueFirst(CoordinatorEvent event) throws RejectedExecutionException {
        queue.addFirst(event);
        processQueue();
    }

    @Override
    public void close() {}

    private void processQueue() {
        if (inEvent) {
            return;
        }

        inEvent = true;
        while (!queue.isEmpty()) {
            CoordinatorEvent event = queue.removeFirst();
            try {
                event.run();
            } catch (Throwable ex) {
                event.complete(ex);
            }
        }
        inEvent = false;
    }
}
