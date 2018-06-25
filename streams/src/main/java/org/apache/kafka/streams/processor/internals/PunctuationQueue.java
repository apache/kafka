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

import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;

import java.util.PriorityQueue;

public class PunctuationQueue {

    private final PriorityQueue<PunctuationSchedule> pq = new PriorityQueue<>();

    public Cancellable schedule(PunctuationSchedule sched) {
        synchronized (pq) {
            pq.add(sched);
        }
        return sched.cancellable();
    }

    public void close() {
        synchronized (pq) {
            pq.clear();
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    boolean mayPunctuate(final long timestamp, final PunctuationType type, final ProcessorNodePunctuator processorNodePunctuator) {
        synchronized (pq) {
            boolean punctuated = false;
            PunctuationSchedule top = pq.peek();
            while (top != null && top.timestamp <= timestamp) {
                PunctuationSchedule sched = top;
                pq.poll();

                if (!sched.isCancelled()) {
                    processorNodePunctuator.punctuate(sched.node(), timestamp, type, sched.punctuator());
                    // sched can be cancelled from within the punctuator
                    if (!sched.isCancelled()) {
                        pq.add(sched.next(timestamp));
                    }
                    punctuated = true;
                }


                top = pq.peek();
            }

            return punctuated;
        }
    }

}
