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

package org.apache.kafka.stream.internals;

import org.apache.kafka.stream.Chooser;

import java.util.ArrayDeque;

public class RoundRobinChooser implements Chooser {

    private final ArrayDeque<RecordQueue> deque;

    public RoundRobinChooser() {
        deque = new ArrayDeque<>();
    }

    @Override
    public void add(RecordQueue queue) {
        deque.offer(queue);
    }

    @Override
    public RecordQueue next() {
        return deque.poll();
    }

    @Override
    public void close() {
        deque.clear();
    }

}
