/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.processor.internals;


import org.apache.kafka.clients.processor.Chooser;
import org.apache.kafka.clients.processor.RecordQueue;

import java.util.Comparator;
import java.util.PriorityQueue;

public class TimeBasedChooser implements Chooser {

    private final PriorityQueue<RecordQueue> pq;

    public TimeBasedChooser() {
        this(new Comparator<RecordQueue>() {
            public int compare(RecordQueue queue1, RecordQueue queue2) {
                long time1 = queue1.trackedTimestamp();
                long time2 = queue2.trackedTimestamp();

                if (time1 < time2) return -1;
                if (time1 > time2) return 1;
                return 0;
            }
        });
    }

    private TimeBasedChooser(Comparator<RecordQueue> comparator) {
        pq = new PriorityQueue<>(3, comparator);
    }

    @Override
    public void add(RecordQueue queue) {
        pq.offer(queue);
    }

    @Override
    public RecordQueue next() {
        return pq.poll();
    }

    @Override
    public void close() {
        pq.clear();
    }

}
